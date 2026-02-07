import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.config import load_config
from src.pipeline.base import Pipeline, PipelineContext
from src.pipeline.ingest import CsvIngestStage
from src.pipeline.validate import ValidateStage
from src.pipeline.transform import TransformStage
from src.pipeline.write import WriteStage
from src.utils.logging import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the tax data pipeline.")
    parser.add_argument(
        "--config",
        default="configs/pipeline.yaml",
        help="Path to pipeline configuration file.",
    )
    parser.add_argument(
        "--allow-backfill",
        action="store_true",
        help="Process data older than the current incremental watermark.",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    config = load_config(Path(args.config))
    if args.allow_backfill:
        config.incremental["allow_backfill"] = True

    pipeline = Pipeline(
        stages=[
            CsvIngestStage(),
            ValidateStage(),
            TransformStage(),
            WriteStage(),
        ]
    )

    context = PipelineContext(config=config)
    sg_tz = ZoneInfo("Asia/Singapore")
    run_timestamp = datetime.now(tz=sg_tz).isoformat()
    context.artifacts["run_id"] = f"run_{datetime.now(tz=sg_tz).strftime('%Y%m%dT%H%M%S%z')}"
    context.artifacts["run_timestamp"] = run_timestamp
    pipeline.run(context)


if __name__ == "__main__":
    main()
