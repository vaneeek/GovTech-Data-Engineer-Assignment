from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass
class PipelineConfig:
    input_path: str
    output_dir: str
    source_name: str
    layers: Dict[str, str]
    archive_dir: str
    columns: Dict[str, str]
    required_columns: List[str]
    quality_tolerance: Dict[str, float]
    incremental: Dict[str, Any]


def load_config(path: Path) -> PipelineConfig:
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    return PipelineConfig(
        input_path=raw["input_path"],
        output_dir=raw["output_dir"],
        source_name=raw.get("source_name", "unknown_source"),
        layers=raw.get("layers", {}),
        archive_dir=raw.get("archive_dir", "archive"),
        columns=raw.get("columns", {}),
        required_columns=raw.get("required_columns", []),
        quality_tolerance=raw.get("quality_tolerance", {}),
        incremental=raw.get("incremental", {}),
    )
