from __future__ import annotations

import hashlib

import pandas as pd


def _stable_return_key(nric: str, assessment_year: str) -> int | pd.NA:
    if not nric or not assessment_year:
        return pd.NA
    digest = hashlib.sha256(f"{nric}:{assessment_year}".encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def build_fact_tax_returns(df: pd.DataFrame, dim_taxpayer: pd.DataFrame, config) -> pd.DataFrame:
    df = df.drop(columns=["taxpayer_id"], errors="ignore")
    for col in [
        "nric",
        "assessment_year",
        "filing_date",
        "annual_income",
        "total_reliefs",
        "chargeable_income",
        "cpf_contribution",
        "foreign_income",
        "tax_payable",
        "tax_paid",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    lookup = dim_taxpayer[["taxpayer_id", "nric"]]
    fact = df.merge(lookup, on="nric", how="left")

    if "taxpayer_id" not in fact.columns:
        fact["taxpayer_id"] = pd.NA

    fact = fact[
        [
            "taxpayer_id",
            "nric",
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
    ].copy()

    fact["assessment_year"] = fact["assessment_year"].astype("Int64")
    fact["return_key"] = fact.apply(
        lambda row: _stable_return_key(
            str(row.get("nric")) if pd.notna(row.get("nric")) else "",
            str(row.get("assessment_year")) if pd.notna(row.get("assessment_year")) else "",
        ),
        axis=1,
    )

    fact.drop(columns=["nric"], inplace=True)
    fact.insert(0, "return_id", range(1, len(fact) + 1))
    return fact
