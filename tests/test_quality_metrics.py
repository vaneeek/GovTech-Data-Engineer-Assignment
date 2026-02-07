import pandas as pd

from src.quality.metrics import calculate_domain_metrics


def test_domain_metrics():
    df = pd.DataFrame(
        {
            "rule_a": [True, False, True],
            "rule_b": [True, True, True],
        }
    )
    domain_map = {"accuracy": ["rule_a", "rule_b"]}
    metrics = calculate_domain_metrics(df, domain_map)

    assert metrics.iloc[0]["passing_rows"] == 2
    assert metrics.iloc[0]["total_rows"] == 3
    assert metrics.iloc[0]["score_pct"] == round((2 / 3) * 100, 2)
