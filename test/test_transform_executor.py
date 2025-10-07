import importlib
import pandas as pd
import numpy as np

transform_executor = importlib.import_module("transform_executor")

def test_parse_condition_is_null_and_compare():
    df = pd.DataFrame({
        "a": [1, None, 3, None],
        "b": ["USD", "EUR", "USD", None],
        "x": [10, 20, 30, 40],
    })
    mask = transform_executor._parse_condition(df, "a IS NULL and b == 'EUR'")
    assert mask.tolist() == [False, True, False, False]
    mask2 = transform_executor._parse_condition(df, "b IS NOT NULL")
    assert mask2.tolist() == [True, True, True, False]
    mask3 = transform_executor._parse_condition(df, "x >= 30")
    assert mask3.tolist() == [False, False, True, True]

def test_eval_expr_arithmetic_and_columns():
    df = pd.DataFrame({"q": [2, 3], "p": [5.0, 10.0]})
    v = transform_executor._eval_expr(df, "q * p")
    assert (v.values == np.array([10.0, 30.0])).all()
    v2 = transform_executor._eval_expr(df, "q * p + 5")
    assert (v2.values == np.array([15.0, 35.0])).all()
    v3 = transform_executor._eval_expr(df, "10 + 2 * 3")
    assert v3 == 16

def test_apply_policies_assign_impute_compute():
    df = pd.DataFrame({
        "status": [None, "open", None],
        "quantity": [1, None, 3],
        "unit_price": [10.0, 20.0, None],
    })
    rules = [
        {"if": "status IS NULL", "then": "assign status = 'unknown'"},
        {"if": "quantity IS NULL", "then": "impute quantity = 1"},
        {"if": "total_amount IS NULL and quantity IS NOT NULL and unit_price IS NOT NULL", "then": "compute total_amount = quantity * unit_price"},
    ]
    out, audit = transform_executor.apply_policies(df.copy(), rules)
    assert audit["assign"]["status"] == 2
    assert audit["impute"]["quantity"] == 1
    assert audit["compute"]["total_amount"] == 2
    assert out.loc[0, "total_amount"] == 10.0
