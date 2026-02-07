from __future__ import annotations

import pandas as pd


def rule_nric_format(df: pd.DataFrame, column: str) -> pd.Series:
    series = df[column].astype("string")
    return series.str.fullmatch(r"[STFG]\d{7}[A-Z]", na=False)


def rule_postal_code(df: pd.DataFrame, column: str) -> pd.Series:
    series = df[column].astype("string")
    return series.str.fullmatch(r"\d{6}", na=False)


def rule_filing_date_after_assessment(df: pd.DataFrame, date_col: str, year_col: str) -> pd.Series:
    filing_date = pd.to_datetime(df[date_col], errors="coerce")
    assessment_year = pd.to_numeric(df[year_col], errors="coerce")

    year_start = pd.to_datetime(assessment_year.astype("Int64").astype(str) + "-12-31", errors="coerce")
    return filing_date > year_start


def rule_chargeable_income(
    df: pd.DataFrame,
    annual_col: str,
    relief_col: str,
    chargeable_col: str,
    tolerance: float,
) -> pd.Series:
    annual = pd.to_numeric(df[annual_col], errors="coerce")
    relief = pd.to_numeric(df[relief_col], errors="coerce")
    chargeable = pd.to_numeric(df[chargeable_col], errors="coerce")

    diff = (annual - relief) - chargeable
    return diff.abs() <= tolerance


def rule_cpf_residency(df: pd.DataFrame, cpf_col: str, residency_col: str) -> pd.Series:
    cpf = pd.to_numeric(df[cpf_col], errors="coerce")
    residency = df[residency_col].astype("string").str.lower()

    is_resident = residency.eq("resident")
    is_non_resident = residency.isin(["non-resident", "nonresident"])
    status_known = is_resident | is_non_resident

    resident_ok = is_resident & cpf.fillna(0).gt(0)
    non_resident_ok = is_non_resident

    return status_known & (resident_ok | non_resident_ok)
