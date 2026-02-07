import logging
from datetime import datetime
from pathlib import Path
import shutil
from typing import Optional

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from src.pipeline.base import PipelineContext, PipelineStage
import json
from zoneinfo import ZoneInfo

from src.quality.outputs import (
    build_quality_outputs,
    build_quarantine_reports,
    build_summary_report,
)
from src.state import write_state

LOGGER = logging.getLogger(__name__)


def _json_safe(value):
    if isinstance(value, (list, dict)):
        return value
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _sanitize_records(records):
    return [{key: _json_safe(value) for key, value in record.items()} for record in records]


def _normalize_datetime(frame: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        if col in frame.columns:
            frame[col] = (
                pd.to_datetime(frame[col], errors="coerce", utc=True)
                .dt.tz_convert("Asia/Singapore")
            )


def _preserve_created_fields(
    existing: pd.DataFrame, new_df: pd.DataFrame, keys: list[str]
) -> pd.DataFrame:
    created_cols = [
        col
        for col in ["created_run_id", "created_at"]
        if col in new_df.columns and col in existing.columns
    ]
    if not created_cols:
        return new_df
    existing_created = existing[keys + created_cols].drop_duplicates(
        subset=keys, keep="first"
    )
    merged = new_df.merge(existing_created, on=keys, how="left", suffixes=("", "_existing"))
    for col in created_cols:
        existing_col = f"{col}_existing"
        if existing_col in merged.columns:
            merged[col] = merged[existing_col].combine_first(merged[col])
            merged.drop(columns=[existing_col], inplace=True)
    return merged


def _align_merge_key_dtypes(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> None:
    for col in keys:
        if col not in left.columns or col not in right.columns:
            continue
        if not is_datetime64_any_dtype(left[col]) or not is_datetime64_any_dtype(right[col]):
            continue
        left_tz = getattr(left[col].dtype, "tz", None)
        right_tz = getattr(right[col].dtype, "tz", None)
        if left_tz and not right_tz:
            right[col] = pd.to_datetime(right[col], errors="coerce").dt.tz_localize(left_tz)
        elif right_tz and not left_tz:
            right[col] = pd.to_datetime(right[col], errors="coerce").dt.tz_convert(None)


def _upsert_parquet(
    path: Path, new_df: pd.DataFrame, keys: list[str], current_run_id: str
) -> None:
    if new_df is None:
        return
    if path.exists():
        existing = pd.read_parquet(path)
        new_frame = _preserve_created_fields(existing, new_df.copy(), keys)
        combined = pd.concat([existing, new_frame], ignore_index=True)
    else:
        combined = new_df.copy()

    if combined.empty:
        return

    if "updated_at" in combined.columns:
        combined = combined.sort_values(by="updated_at")

    combined = combined.drop_duplicates(subset=keys, keep="last")

    if "last_seen_run_id" in combined.columns and not new_df.empty:
        key_df = new_df[keys].drop_duplicates()
        key_df["_present"] = True
        combined = combined.merge(key_df, on=keys, how="left")
        combined.loc[combined["_present"].eq(True), "last_seen_run_id"] = current_run_id
        combined.drop(columns=["_present"], inplace=True)
    combined.to_parquet(path, index=False)


def _upsert_fact_scd2(
    path: Path,
    new_df: pd.DataFrame,
    current_run_id: str,
    current_run_ts: datetime,
) -> None:
    if new_df is None:
        return
    existing_current = None
    dedupe_cols = []
    if path.exists():
        existing = pd.read_parquet(path)
        date_cols = ["filing_date", "created_at", "updated_at"]
        new_frame = new_df.copy()
        _normalize_datetime(existing, date_cols)
        _normalize_datetime(new_frame, date_cols)
        dedupe_cols = [
            "return_key",
            "taxpayer_id",
            "assessment_year",
            "filing_date",
            "annual_income",
            "total_reliefs",
            "chargeable_income",
            "cpf_contribution",
            "foreign_income",
            "tax_payable",
            "tax_paid",
        ]
        dedupe_cols = [
            col for col in dedupe_cols if col in new_frame.columns and col in existing.columns
        ]
        if "is_current" in existing.columns:
            existing_current = existing[existing["is_current"]].copy()
        if dedupe_cols:
            new_frame = _preserve_created_fields(existing, new_frame, dedupe_cols)
        combined = pd.concat([existing, new_frame], ignore_index=True)
    else:
        combined = new_df.copy()

    if combined.empty:
        return

    _normalize_datetime(combined, ["filing_date", "created_at", "updated_at"])

    if not dedupe_cols:
        dedupe_cols = [
            "return_key",
            "taxpayer_id",
            "assessment_year",
            "filing_date",
            "annual_income",
            "total_reliefs",
            "chargeable_income",
            "cpf_contribution",
            "foreign_income",
            "tax_payable",
            "tax_paid",
        ]
        dedupe_cols = [col for col in dedupe_cols if col in combined.columns]
    combined = combined.drop_duplicates(subset=dedupe_cols, keep="last")

    combined = combined.sort_values(by=["return_key", "filing_date", "updated_at"], na_position="last")
    combined["version"] = combined.groupby("return_key").cumcount() + 1

    if "last_seen_run_id" in combined.columns and dedupe_cols and not new_df.empty:
        if "new_frame" in locals():
            present_rows = new_frame[dedupe_cols].drop_duplicates()
        else:
            present_rows = new_df[dedupe_cols].drop_duplicates()
        present_rows["_present"] = True
        _align_merge_key_dtypes(combined, present_rows, dedupe_cols)
        combined = combined.merge(present_rows, on=dedupe_cols, how="left")
        combined.loc[combined["_present"].eq(True), "last_seen_run_id"] = current_run_id
        combined.drop(columns=["_present"], inplace=True)

    effective_start = combined["filing_date"].fillna(combined["created_at"])
    combined["effective_start"] = effective_start
    combined["effective_end"] = combined.groupby("return_key")["effective_start"].shift(-1)
    combined["is_current"] = combined["effective_end"].isna()
    combined.loc[combined["is_current"], "effective_end"] = pd.Timestamp(
        "2262-04-11", tz="Asia/Singapore"
    )

    if existing_current is not None and dedupe_cols:
        current_keys = existing_current[dedupe_cols].drop_duplicates()
        current_keys["_was_current"] = True
        combined = combined.merge(current_keys, on=dedupe_cols, how="left")
        retired_mask = combined["_was_current"].eq(True) & ~combined["is_current"]
        if "updated_at" not in combined.columns:
            combined["updated_at"] = pd.NaT
        combined.loc[retired_mask, "updated_at"] = current_run_ts
        combined.drop(columns=["_was_current"], inplace=True)

    combined.to_parquet(path, index=False)


def _upsert_dim_taxpayer_scd2(
    path: Path,
    new_df: pd.DataFrame,
    current_run_id: str,
    current_run_ts: datetime,
) -> None:
    if new_df is None:
        return
    existing_current = None
    dedupe_cols = []
    if path.exists():
        existing = pd.read_parquet(path)
        date_cols = ["created_at", "updated_at"]
        new_frame = new_df.copy()
        _normalize_datetime(existing, date_cols)
        _normalize_datetime(new_frame, date_cols)
        dedupe_cols = [
            "nric",
            "full_name",
            "filing_status",
            "residential_status",
            "number_of_dependents",
            "occupation",
            "postal_code",
            "housing_type",
            "geo_id",
        ]
        dedupe_cols = [
            col for col in dedupe_cols if col in new_frame.columns and col in existing.columns
        ]
        if "is_current" in existing.columns:
            existing_current = existing[existing["is_current"]].copy()
        if dedupe_cols:
            new_frame = _preserve_created_fields(existing, new_frame, dedupe_cols)
        combined = pd.concat([existing, new_frame], ignore_index=True)
    else:
        combined = new_df.copy()

    if combined.empty:
        return

    _normalize_datetime(combined, ["created_at", "updated_at"])

    if not dedupe_cols:
        dedupe_cols = [
            "nric",
            "full_name",
            "filing_status",
            "residential_status",
            "number_of_dependents",
            "occupation",
            "postal_code",
            "housing_type",
            "geo_id",
        ]
        dedupe_cols = [col for col in dedupe_cols if col in combined.columns]
    combined = combined.drop_duplicates(subset=dedupe_cols, keep="last")

    combined = combined.sort_values(by=["nric", "updated_at"], na_position="last")
    combined["version"] = combined.groupby("nric").cumcount() + 1

    if "last_seen_run_id" in combined.columns and dedupe_cols and not new_df.empty:
        if "new_frame" in locals():
            present_rows = new_frame[dedupe_cols].drop_duplicates()
        else:
            present_rows = new_df[dedupe_cols].drop_duplicates()
        present_rows["_present"] = True
        combined = combined.merge(present_rows, on=dedupe_cols, how="left")
        combined.loc[combined["_present"].eq(True), "last_seen_run_id"] = current_run_id
        combined.drop(columns=["_present"], inplace=True)

    combined["effective_start"] = combined["updated_at"]
    combined["effective_end"] = combined.groupby("nric")["effective_start"].shift(-1)
    combined["is_current"] = combined["effective_end"].isna()
    combined.loc[combined["is_current"], "effective_end"] = pd.Timestamp(
        "2262-04-11", tz="Asia/Singapore"
    )

    if existing_current is not None and dedupe_cols:
        current_keys = existing_current[dedupe_cols].drop_duplicates()
        current_keys["_was_current"] = True
        combined = combined.merge(current_keys, on=dedupe_cols, how="left")
        retired_mask = combined["_was_current"].eq(True) & ~combined["is_current"]
        combined.loc[retired_mask, "updated_at"] = current_run_ts
        combined.drop(columns=["_was_current"], inplace=True)

    combined.to_parquet(path, index=False)


def _parse_file_id(file_id: str) -> dict:
    parts = file_id.split(":")
    if len(parts) != 3:
        return {"file_name": file_id, "file_size": None, "file_mtime": None}
    name, size, mtime = parts
    try:
        file_size = int(size)
    except ValueError:
        file_size = None
    try:
        file_mtime = datetime.utcfromtimestamp(int(mtime)).replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Singapore")).isoformat()
    except ValueError:
        file_mtime = None
    return {"file_name": name, "file_size": file_size, "file_mtime": file_mtime}


class WriteStage(PipelineStage):
    name = "write"

    def run(self, context: PipelineContext, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        output_dir = Path(context.config.output_dir)
        layers = context.config.layers
        source_name = context.config.source_name
        landing_dir = Path(layers.get("landing_dir", output_dir / "landing"))
        raw_zone = Path(layers.get("raw_dir", output_dir / "raw"))
        staging_zone = Path(layers.get("staging_dir", output_dir / "staging"))
        curated_zone = Path(layers.get("curated_dir", output_dir / "curated"))
        datamart_zone = Path(layers.get("datamart_dir", output_dir / "datamart"))
        quarantine_zone = staging_zone / "quarantine"
        run_id = context.artifacts.get("run_id") or "run_unknown"
        run_timestamp = context.artifacts.get("run_timestamp")

        run_dt = datetime.now(tz=ZoneInfo("Asia/Singapore"))
        if run_timestamp:
            try:
                run_dt = datetime.fromisoformat(run_timestamp)
            except ValueError:
                run_dt = datetime.now(tz=ZoneInfo("Asia/Singapore"))

        ingest_date = run_dt.date().isoformat()

        for zone in [landing_dir, raw_zone, staging_zone, quarantine_zone, curated_zone, datamart_zone]:
            zone.mkdir(parents=True, exist_ok=True)

        dim_taxpayer = context.artifacts.get("dim_taxpayer")
        dim_geo = context.artifacts.get("dim_geo")
        fact_tax_returns = context.artifacts.get("fact_tax_returns")
        quality_metrics = context.artifacts.get("quality_metrics")
        validated = context.artifacts.get("validated")
        staging = context.artifacts.get("staging")
        quarantine = context.artifacts.get("quarantine")
        raw = context.artifacts.get("raw")

        if isinstance(raw, pd.DataFrame):
            raw_frame = raw.copy()
            raw_frame["created_run_id"] = run_id
            raw_frame["last_seen_run_id"] = run_id
            raw_frame["ingested_at"] = run_timestamp
            raw_part_dir = raw_zone / source_name / f"ingest_date={ingest_date}"
            raw_part_dir.mkdir(parents=True, exist_ok=True)
            raw_frame.to_parquet(raw_part_dir / f"raw_{run_id}.parquet", index=False)
        if isinstance(staging, pd.DataFrame):
            staging_frame = staging.copy()
            staging_frame["created_run_id"] = run_id
            staging_frame["last_seen_run_id"] = run_id
            staging_frame["ingested_at"] = run_timestamp
            staging_part_dir = staging_zone / source_name / f"ingest_date={ingest_date}"
            staging_part_dir.mkdir(parents=True, exist_ok=True)
            staging_frame.to_parquet(staging_part_dir / f"staging_{run_id}.parquet", index=False)
        if isinstance(quarantine, pd.DataFrame):
            quarantine_frame = quarantine.copy()
            quarantine_frame["created_run_id"] = run_id
            quarantine_frame["last_seen_run_id"] = run_id
            quarantine_frame["ingested_at"] = run_timestamp
            quarantine_part_dir = quarantine_zone / source_name / f"ingest_date={ingest_date}"
            quarantine_part_dir.mkdir(parents=True, exist_ok=True)
            quarantine_frame.to_parquet(
                quarantine_part_dir / f"quarantine_{run_id}.parquet", index=False
            )
            breakdown, samples = build_quarantine_reports(quarantine)
            breakdown.to_parquet(
                quarantine_part_dir / f"quarantine_breakdown_{run_id}.parquet",
                index=False,
            )
            samples.to_parquet(
                quarantine_part_dir / f"quarantine_samples_{run_id}.parquet",
                index=False,
            )

        for source_path in context.artifacts.get("source_files", []):
            landing_path = landing_dir / source_name / run_dt.strftime("%Y/%m/%d") / run_id
            landing_path.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, landing_path / source_path.name)

            archive_root = context.artifacts.get("archive_dir", Path("archive"))
            archive_path = archive_root / source_name / run_dt.strftime("%Y/%m/%d") / run_id
            archive_path.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), archive_path / source_path.name)

        if isinstance(dim_taxpayer, pd.DataFrame):
            _upsert_dim_taxpayer_scd2(
                curated_zone / "dim_taxpayer.parquet",
                dim_taxpayer,
                run_id,
                run_dt,
            )
        if isinstance(dim_geo, pd.DataFrame):
            _upsert_parquet(
                curated_zone / "dim_geo.parquet",
                dim_geo,
                ["postal_code"],
                run_id,
            )
        if isinstance(fact_tax_returns, pd.DataFrame):
            _upsert_fact_scd2(
                curated_zone / "fact_tax_returns.parquet",
                fact_tax_returns,
                run_id,
                run_dt,
            )

        data_quality_results, agg_metrics = build_quality_outputs(validated, quality_metrics)
        data_quality_results.to_parquet(curated_zone / "data_quality_results.parquet", index=False)
        agg_metrics.to_parquet(curated_zone / "agg_data_quality_metrics.parquet", index=False)

        if isinstance(validated, pd.DataFrame):
            summary = build_summary_report(validated)
            if isinstance(quarantine, pd.DataFrame):
                breakdown, samples = build_quarantine_reports(quarantine)
                summary["quarantine_breakdown"] = _sanitize_records(
                    breakdown.to_dict(orient="records")
                )
                summary["quarantine_samples"] = _sanitize_records(
                    samples.to_dict(orient="records")
                )
            summary = {key: _json_safe(value) for key, value in summary.items()}
            pd.DataFrame([summary]).to_parquet(
                curated_zone / "summary_report.parquet", index=False
            )

        if isinstance(dim_taxpayer, pd.DataFrame) and isinstance(dim_geo, pd.DataFrame) and isinstance(fact_tax_returns, pd.DataFrame):
            dim_taxpayer_current = dim_taxpayer
            dim_taxpayer_path = curated_zone / "dim_taxpayer.parquet"
            if dim_taxpayer_path.exists():
                dim_taxpayer_loaded = pd.read_parquet(dim_taxpayer_path)
                if "is_current" in dim_taxpayer_loaded.columns:
                    dim_taxpayer_current = dim_taxpayer_loaded[dim_taxpayer_loaded["is_current"]].copy()
            mart = fact_tax_returns.merge(
                dim_taxpayer_current,
                on="taxpayer_id",
                how="left",
                suffixes=("", "_taxpayer"),
            ).merge(dim_geo, on="geo_id", how="left", suffixes=("", "_geo"))
            mart.to_parquet(datamart_zone / "datamart_tax_returns.parquet", index=False)

        incremental_cfg = context.config.incremental or {}
        if incremental_cfg.get("enabled"):
            state_path = context.artifacts.get("incremental_state_path")
            max_year = context.artifacts.get("incremental_max_year")
            processed_files = context.artifacts.get("incremental_processed_files") or []
            new_files = context.artifacts.get("incremental_new_files") or []
            if state_path:
                last_year = context.artifacts.get("incremental_last_year")
                next_max = last_year
                if max_year is not None:
                    next_max = max(filter(lambda x: isinstance(x, int), [last_year, max_year]), default=max_year)
                state_payload = {
                    "last_assessment_year": next_max,
                    "processed_files": sorted(set(processed_files + new_files)),
                }
                write_state(Path(state_path), state_payload)

            metadata_dir = output_dir / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)
            all_files = sorted(set(processed_files + new_files))
            processed_at = datetime.now(tz=ZoneInfo("Asia/Singapore")).isoformat()
            ledger_rows = []
            for file_id in all_files:
                parsed = _parse_file_id(file_id)
                ledger_rows.append(
                    {
                        "file_id": file_id,
                        "file_name": parsed["file_name"],
                        "file_size": parsed["file_size"],
                        "file_mtime": parsed["file_mtime"],
                        "processed_at": processed_at,
                        "is_new": file_id in new_files,
                    }
                )
            pd.DataFrame(ledger_rows).to_csv(
                metadata_dir / "processed_files.csv", index=False
            )

        LOGGER.info("Outputs written to %s", output_dir)
        return data
