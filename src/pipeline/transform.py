import logging
from typing import Optional

import pandas as pd

from src.pipeline.base import PipelineContext, PipelineStage
from src.transform.dimensions import build_dim_geo, build_dim_taxpayer
from src.transform.facts import build_fact_tax_returns

LOGGER = logging.getLogger(__name__)


class TransformStage(PipelineStage):
    name = "transform"

    def run(self, context: PipelineContext, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if data is None:
            raise ValueError("Transform stage requires input data.")

        run_id = context.artifacts.get("run_id")
        run_timestamp = context.artifacts.get("run_timestamp")
        source = context.artifacts.get("valid", data)

        dim_geo = build_dim_geo(source)
        dim_taxpayer = build_dim_taxpayer(source, dim_geo)
        fact_tax_returns = build_fact_tax_returns(source, dim_taxpayer, context.config)

        for frame in [dim_geo, dim_taxpayer, fact_tax_returns]:
            if run_id is not None:
                frame["created_run_id"] = run_id
                frame["last_seen_run_id"] = run_id
            if run_timestamp is not None:
                frame["created_at"] = run_timestamp
                frame["updated_at"] = run_timestamp

        context.artifacts["dim_geo"] = dim_geo
        context.artifacts["dim_taxpayer"] = dim_taxpayer
        context.artifacts["fact_tax_returns"] = fact_tax_returns
        return data
