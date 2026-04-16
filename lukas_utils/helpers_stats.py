import pandas as pd
from statsmodels.tsa.stattools import adfuller


def adf_test_summary(ser):
    # ADF H0: there is a unit root

    specs = {
        "constant": "c",
        "constant trend": "ct",
        "constant ltend, qtrend": "ctt",
        "none": "n",
    }
    results = {}

    for pretty, spec in specs.items():
        adf, pval, ulag, nobs, cval, icb = adfuller(ser, regression=spec)
        keys = ["adf-stat", "p-value", "lags", "obs", *cval.keys(), "inf crit"]
        res = [adf, pval, ulag, nobs, *cval.values(), icb]
        results[pretty] = dict(zip(keys, res))

    if ser.name is not None:
        title = ser.name.upper()
    else:
        title = ""

    print("-" * 77)
    print(f"ADF Test {title}: H0 there is a unit root")
    print("-" * 77)
    print(pd.DataFrame(results).transpose().round(3).iloc[:, :-1])
    print("\n")

    pass


def winsorise(ser, p: float = 0.01):
    ser = ser.copy()
    lower, upper = ser.quantile(p), ser.quantile(1 - p)

    flt = ser <= lower
    ser.loc[flt] = [lower] * flt.sum()

    flt = ser >= upper
    ser.loc[flt] = [upper] * flt.sum()

    return ser
