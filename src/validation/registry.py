from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from src.validation import rules


@dataclass
class RuleDefinition:
    name: str
    domain: str
    series: pd.Series


class RuleRegistry:
    def __init__(
        self,
        domain_map: Dict[str, List[str]],
        rules_list: List[RuleDefinition],
        required_columns: List[str],
        tolerance: float,
    ) -> None:
        self.domain_map = domain_map
        self.rules_list = rules_list
        self.required_columns = required_columns
        self.tolerance = tolerance

    @classmethod
    def from_config(cls, config) -> "RuleRegistry":
        domain_map = {
            "completeness": ["rule_nric_format"],
            "validity": ["rule_postal_code"],
            "accuracy": ["rule_filing_date_after_assessment", "rule_chargeable_income", "rule_cpf_residency"],
        }
        tolerance = config.quality_tolerance.get("income_diff", 0.01)

        rules_list: List[RuleDefinition] = []
        return cls(domain_map, rules_list, config.required_columns, tolerance)

    def apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = df.columns
        for col in self.required_columns:
            if col not in cols:
                df[col] = pd.NA

        df = df.copy()
        df["row_id"] = range(1, len(df) + 1)

        df["rule_nric_format"] = rules.rule_nric_format(df, "nric")
        df["rule_postal_code"] = rules.rule_postal_code(df, "postal_code")
        df["rule_filing_date_after_assessment"] = rules.rule_filing_date_after_assessment(
            df, "filing_date", "assessment_year"
        )
        df["rule_chargeable_income"] = rules.rule_chargeable_income(
            df, "annual_income", "total_reliefs", "chargeable_income", tolerance=self.tolerance
        )
        df["rule_cpf_residency"] = rules.rule_cpf_residency(
            df, "cpf_contribution", "residential_status"
        )

        df["dq_completeness_pass"] = df["rule_nric_format"]
        df["dq_validity_pass"] = df["rule_postal_code"]
        df["dq_accuracy_pass"] = df[
            ["rule_filing_date_after_assessment", "rule_chargeable_income", "rule_cpf_residency"]
        ].all(axis=1)

        return df
