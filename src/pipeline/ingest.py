import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from src.pipeline.base import PipelineContext, PipelineStage
from src.state import read_state

LOGGER = logging.getLogger(__name__)


class CsvIngestStage(PipelineStage):
    name = "ingest_csv"

    def run(self, context: PipelineContext, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        input_path = Path(context.config.input_path)
        incremental_cfg = context.config.incremental or {}
        df, new_file_ids, processed_files, source_files = self._read_input(
            input_path,
            incremental_cfg,
        )

        df.columns = [self._normalize_column(col) for col in df.columns]
        rename_map = {
            self._normalize_column(source): target
            for target, source in context.config.columns.items()
            if source
        }
        df = df.rename(columns=rename_map)
        context.artifacts["raw"] = df

        context.artifacts["source_files"] = source_files
        context.artifacts["archive_dir"] = Path(context.config.archive_dir)

        if incremental_cfg.get("enabled"):
            key = incremental_cfg.get("key", "assessment_year")
            state_path = Path(incremental_cfg.get("state_path", "outputs/metadata/state.json"))
            state = read_state(state_path)
            last_year = state.get("last_assessment_year")
            allow_backfill = incremental_cfg.get("allow_backfill", False)

            if key not in df.columns:
                df[key] = pd.NA
            df[key] = pd.to_numeric(df[key], errors="coerce")
            if last_year is not None and not allow_backfill:
                df = df[(df[key].isna()) | (df[key] >= last_year)].copy()
            current_max = df[key].dropna().max() if key in df.columns else None
            context.artifacts["incremental_state_path"] = state_path
            context.artifacts["incremental_last_year"] = last_year
            context.artifacts["incremental_max_year"] = int(current_max) if pd.notna(current_max) else None
            context.artifacts["incremental_processed_files"] = processed_files
            context.artifacts["incremental_new_files"] = new_file_ids

        return df

    def _read_input(
        self,
        input_path: Path,
        incremental_cfg: dict,
    ) -> tuple[pd.DataFrame, list[str], list[str], list[Path]]:
        track_files = incremental_cfg.get("track_files", False)
        state_path = Path(incremental_cfg.get("state_path", "outputs/metadata/state.json"))
        state = read_state(state_path) if incremental_cfg.get("enabled") else {}
        processed_files = state.get("processed_files", [])
        processed_set = set(processed_files)
        new_file_ids: list[str] = []
        source_files: list[Path] = []

        def file_id(path: Path) -> str:
            stat = path.stat()
            return f"{path.name}:{stat.st_size}:{int(stat.st_mtime)}"

        if input_path.is_dir():
            csv_files = sorted(input_path.glob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found in {input_path}")
            frames = []
            for file_path in csv_files:
                file_key = file_id(file_path)
                if track_files and file_key in processed_set:
                    continue
                LOGGER.info("Reading input file: %s", file_path)
                frame = pd.read_csv(file_path)
                frame["source_file"] = file_path.name
                frame["source_file_id"] = file_key
                frames.append(frame)
                new_file_ids.append(file_key)
                source_files.append(file_path)
            if not frames:
                return pd.DataFrame(), new_file_ids, processed_files, source_files
            return pd.concat(frames, ignore_index=True), new_file_ids, processed_files, source_files

        LOGGER.info("Reading input file: %s", input_path)
        file_key = file_id(input_path)
        if track_files and file_key in processed_set:
            return pd.DataFrame(), new_file_ids, processed_files, source_files
        new_file_ids.append(file_key)
        frame = pd.read_csv(input_path)
        frame["source_file"] = input_path.name
        frame["source_file_id"] = file_key
        source_files.append(input_path)
        return frame, new_file_ids, processed_files, source_files

    @staticmethod
    def _normalize_column(name: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
        normalized = re.sub(r"_+", "_", normalized)
        return normalized.strip("_")
