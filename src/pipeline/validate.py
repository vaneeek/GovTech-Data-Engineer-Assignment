import logging
from typing import Optional

import pandas as pd

from src.pipeline.base import PipelineContext, PipelineStage
from src.quality.metrics import calculate_domain_metrics
from src.validation.registry import RuleRegistry

LOGGER = logging.getLogger(__name__)


class ValidateStage(PipelineStage):
    name = "validate"

    def run(self, context: PipelineContext, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if data is None:
            raise ValueError("Validation stage requires input data.")

        cleaned = data.copy()
        for col in ["nric", "postal_code", "residential_status", "occupation"]:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].astype("string").str.strip()

        for col in [
            "annual_income",
            "total_reliefs",
            "chargeable_income",
            "cpf_contribution",
            "tax_payable",
            "tax_paid",
        ]:
            if col in cleaned.columns:
                cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")

        if "filing_date" in cleaned.columns:
            cleaned["filing_date"] = pd.to_datetime(cleaned["filing_date"], errors="coerce")

        registry = RuleRegistry.from_config(context.config)
        results = registry.apply_rules(cleaned)
        metrics = calculate_domain_metrics(results, registry.domain_map)

        rule_cols = [col for cols in registry.domain_map.values() for col in cols]
        dq_all_pass = results[rule_cols].all(axis=1) if rule_cols else pd.Series(False, index=results.index)

        staging = results.copy()
        quarantine = results.loc[~dq_all_pass].copy()
        valid = results.loc[dq_all_pass].copy()

        context.artifacts["validated"] = results
        context.artifacts["staging"] = staging
        context.artifacts["quarantine"] = quarantine
        context.artifacts["valid"] = valid
        context.artifacts["quality_metrics"] = metrics
        return results
