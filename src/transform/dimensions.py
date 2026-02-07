from __future__ import annotations

import hashlib

import pandas as pd


def _stable_int_id(value: str, prefix: str) -> int:
    digest = hashlib.sha256(f"{prefix}:{value}".encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _postal_region(postal_code: str) -> str:
    if not isinstance(postal_code, str) or len(postal_code) < 2:
        return "Unknown"

    try:
        sector = int(postal_code[:2])
    except ValueError:
        return "Unknown"

    district_ranges = {
        1: [(1, 6)],
        2: [(7, 8)],
        3: [(14, 16)],
        4: [(9, 10)],
        5: [(11, 13)],
        6: [(17, 17)],
        7: [(18, 19)],
        8: [(20, 21)],
        9: [(22, 23)],
        10: [(24, 27)],
        11: [(28, 30)],
        12: [(31, 33)],
        13: [(34, 37)],
        14: [(38, 41)],
        15: [(42, 45)],
        16: [(46, 48)],
        17: [(49, 50)],
        18: [(51, 52)],
        19: [(53, 55)],
        20: [(56, 57)],
        21: [(58, 59)],
        22: [(60, 64)],
        23: [(65, 68)],
        24: [(69, 71)],
        25: [(72, 73)],
        26: [(77, 78)],
        27: [(75, 76)],
        28: [(79, 80), (81, 82)],
    }

    district = None
    for d, ranges in district_ranges.items():
        if any(start <= sector <= end for start, end in ranges):
            district = d
            break

    if district is None:
        return "Unknown"

    if 1 <= district <= 13:
        return "Central"
    if district in {14, 15, 16, 17, 18}:
        return "East"
    if district in {19, 20, 28}:
        return "Northeast"
    if district in {25, 26, 27}:
        return "North"
    if district in {21, 22, 23, 24}:
        return "West"

    return "Unknown"


def build_dim_geo(df: pd.DataFrame) -> pd.DataFrame:
    if "postal_code" not in df.columns:
        df = df.copy()
        df["postal_code"] = pd.NA

    geo = pd.DataFrame({"postal_code": df["postal_code"].astype("string")})
    geo["region"] = geo["postal_code"].apply(_postal_region)
    geo = geo.drop_duplicates().reset_index(drop=True)

    geo["geo_id"] = geo["postal_code"].fillna("").apply(
        lambda code: _stable_int_id(code, "GEO") if code else pd.NA
    )

    geo = geo[["geo_id", "postal_code", "region"]]
    return geo


def build_dim_taxpayer(df: pd.DataFrame, dim_geo: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "nric",
        "full_name",
        "filing_status",
        "residential_status",
        "number_of_dependents",
        "occupation",
        "postal_code",
        "housing_type",
    ]

    working = df.copy()
    for col in cols:
        if col not in working.columns:
            working[col] = pd.NA

    dim = working[cols].drop_duplicates().reset_index(drop=True)
    dim["taxpayer_id"] = dim["nric"].astype("string").fillna("").apply(
        lambda value: _stable_int_id(value, "NRIC") if value else pd.NA
    )

    dim = dim.merge(dim_geo[["geo_id", "postal_code"]], on="postal_code", how="left")
    dim = dim[
        [
            "taxpayer_id",
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
    ]
    return dim
