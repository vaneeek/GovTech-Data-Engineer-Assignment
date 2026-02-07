from __future__ import annotations

from datetime import datetime
from typing import Tuple

import pandas as pd
from zoneinfo import ZoneInfo


def build_quality_outputs(
    validated: pd.DataFrame, metrics: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sg_tz = ZoneInfo("Asia/Singapore")
    run_id = datetime.now(tz=sg_tz).strftime("run_%Y%m%dT%H%M%S%z")
    run_timestamp = datetime.now(tz=sg_tz).isoformat()

    data_quality_results = validated[
        [
            "row_id",
            "rule_nric_format",
            "rule_postal_code",
            "rule_filing_date_after_assessment",
            "rule_chargeable_income",
            "rule_cpf_residency",
            "dq_completeness_pass",
            "dq_validity_pass",
            "dq_accuracy_pass",
        ]
    ].copy()
    data_quality_results["created_run_id"] = run_id
    data_quality_results["last_seen_run_id"] = run_id
    data_quality_results["run_timestamp"] = run_timestamp

    agg_metrics = metrics.copy()
    agg_metrics["created_run_id"] = run_id
    agg_metrics["last_seen_run_id"] = run_id
    agg_metrics["run_timestamp"] = run_timestamp

    return data_quality_results, agg_metrics


def build_summary_report(validated: pd.DataFrame) -> dict:
    total_rows = len(validated)
    sg_tz = ZoneInfo("Asia/Singapore")

    def count_false(column: str) -> int:
        if column not in validated.columns:
            return 0
        return int((~validated[column].fillna(False)).sum())

    def sum_numeric(column: str) -> float:
        if column not in validated.columns:
            return 0.0
        return float(pd.to_numeric(validated[column], errors="coerce").sum())

    summary = {
        "total_rows": int(total_rows),
        "invalid_nric_count": count_false("rule_nric_format"),
        "invalid_postal_count": count_false("rule_postal_code"),
        "invalid_filing_date_count": count_false("rule_filing_date_after_assessment"),
        "invalid_chargeable_income_count": count_false("rule_chargeable_income"),
        "invalid_cpf_residency_count": count_false("rule_cpf_residency"),
        "annual_income_total": sum_numeric("annual_income"),
        "reliefs_total": sum_numeric("total_reliefs"),
        "chargeable_income_total": sum_numeric("chargeable_income"),
        "tax_payable_total": sum_numeric("tax_payable"),
        "tax_paid_total": sum_numeric("tax_paid"),
        "run_timestamp": datetime.now(tz=sg_tz).isoformat(),
    }

    return summary


def build_quarantine_reports(
    quarantine: pd.DataFrame, sample_size: int = 3
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if quarantine.empty:
        breakdown = pd.DataFrame(columns=["rule", "invalid_count"])
        samples = pd.DataFrame(
            columns=["rule", "sample_row_id", "nric", "postal_code", "filing_date"]
        )
        return breakdown, samples

    rule_cols = [col for col in quarantine.columns if col.startswith("rule_")]
    breakdown_rows = []
    sample_rows = []

    for rule in rule_cols:
        invalid_mask = ~quarantine[rule].fillna(False)
        invalid_count = int(invalid_mask.sum())
        breakdown_rows.append({"rule": rule, "invalid_count": invalid_count})

        if invalid_count:
            sample_df = quarantine.loc[invalid_mask, ["row_id", "nric", "postal_code", "filing_date"]]
            for _, row in sample_df.head(sample_size).iterrows():
                filing_date = row.get("filing_date")
                if pd.notna(filing_date):
                    filing_date = pd.to_datetime(filing_date, errors="coerce")
                    filing_date = filing_date.isoformat() if pd.notna(filing_date) else None
                sample_rows.append(
                    {
                        "rule": rule,
                        "sample_row_id": int(row["row_id"]),
                        "nric": row.get("nric"),
                        "postal_code": row.get("postal_code"),
                        "filing_date": filing_date,
                    }
                )

    breakdown = pd.DataFrame(breakdown_rows).sort_values(
        by="invalid_count", ascending=False
    )
    samples = pd.DataFrame(sample_rows)

    return breakdown, samples
