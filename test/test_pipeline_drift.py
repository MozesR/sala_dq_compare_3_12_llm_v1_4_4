import importlib
import pandas as pd

pipeline = importlib.import_module("pipeline")

def test_iqr_outliers_basic():
    s = pd.Series([1,2,3,4,5,100])
    count, lo, hi = pipeline.iqr_outliers(s, k=1.5)
    assert count == 1
    assert lo < 1.5 and hi > 5

def test_mean_delta():
    assert pipeline.mean_delta(10, 7) == 3.0
    assert pipeline.mean_delta(7, 10) == -3.0
    assert pipeline.mean_delta("10", "x") is None
