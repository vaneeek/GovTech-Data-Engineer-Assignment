from __future__ import annotations

from typing import Dict, List

import pandas as pd


def calculate_domain_metrics(df: pd.DataFrame, domain_map: Dict[str, List[str]]) -> pd.DataFrame:
    metrics = []
    total = len(df)

    for domain, rules in domain_map.items():
        if not rules:
            continue
        passing = df[rules].all(axis=1).sum()
        score = (passing / total) * 100 if total else 0.0
        metrics.append(
            {
                "domain": domain,
                "passing_rows": int(passing),
                "total_rows": int(total),
                "score_pct": round(score, 2),
            }
        )

    return pd.DataFrame(metrics)
