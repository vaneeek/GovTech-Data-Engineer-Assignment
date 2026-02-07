import pandas as pd

from src.validation import rules


def test_rule_nric_format_valid():
    df = pd.DataFrame({"nric": ["S1234567A", "T7654321Z"]})
    result = rules.rule_nric_format(df, "nric")
    assert result.tolist() == [True, True]


def test_rule_nric_format_invalid():
    df = pd.DataFrame({"nric": ["123", None]})
    result = rules.rule_nric_format(df, "nric")
    assert result.tolist() == [False, False]


def test_rule_postal_code():
    df = pd.DataFrame({"postal_code": ["123456", "12345", None]})
    result = rules.rule_postal_code(df, "postal_code")
    assert result.tolist() == [True, False, False]


def test_rule_chargeable_income():
    df = pd.DataFrame(
        {
            "annual_income": [100.0, 100.0],
            "total_reliefs": [10.0, 5.0],
            "chargeable_income": [90.0, 99.0],
        }
    )
    result = rules.rule_chargeable_income(
        df, "annual_income", "total_reliefs", "chargeable_income", 0.01
    )
    assert result.tolist() == [True, False]


def test_rule_cpf_residency():
    df = pd.DataFrame({"cpf": [100.0, 0.0, None], "residency": ["resident", "resident", "non-resident"]})
    result = rules.rule_cpf_residency(df, "cpf", "residency")
    assert result.tolist() == [True, False, True]
