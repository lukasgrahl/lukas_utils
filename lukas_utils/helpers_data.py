import os
import regex as re
import toml
import pandas as pd
import functools as ft

from .helpers_logging import get_logger

MY_LOGGER = get_logger(os.path.basename(__file__))


class DataColumn:
    def __init__(
        self,
        name: str,
        dtype: str,
        freq: str = None,
        dtype_sql: str = None,
        is_default: bool = False,
        **kwargs,
    ):
        dct_tup_freq = {"D": 8, "W": 7, "M": 6, "Q": 5, "Y": 4, None: 0}
        dct_dtype_sql = {
            "float": "double",
            "int": "int",
            "bool": "bool",
            "datetime64[ns]": "datetime",
            "str": "varchar(25)",
        }

        self.freq_ordinary = dct_tup_freq[freq]

        self.dtype_sql = dtype_sql
        if dtype_sql is None:
            if dtype in dct_dtype_sql.keys():
                d = dct_dtype_sql[dtype]
            else:
                d = "varchar(25)"
            self.dtype_sql = d

        self.name = name
        self.dtype = dtype
        self.freq = freq
        self.is_default = is_default
        pass

    def __repr__(self):
        return self.name + ": " + str(self.dtype) + ", " + str(self.freq)


def _get_dtype_toml():
    if "PATH_DTYPE_TOML" not in os.environ.keys():
        MY_LOGGER.error("please add PATH_DTYPE_TOML to os.environ as abspath")
    if "PATH_DTYPE_REGEX_TOML" not in os.environ.keys():
        MY_LOGGER.error("please add PATH_DTYPE_REGEX_TOML to os.environ as abspath")
    dct_dtype = toml.load(os.environ["PATH_DTYPE_TOML"])
    dct_regex = toml.load(os.environ["PATH_DTYPE_REGEX_TOML"])
    return dct_dtype, dct_regex


def _get_dict_freq_dtype_update(
    dct_dtype: dict = None, dct_regex: dict = None
) -> (dict, dict):
    # check dct format
    for dct in [d for d in [dct_dtype, dct_regex] if d is not None]:
        for k, v in dct.items():
            if ("dtype" not in v.keys()) or (
                len(
                    [
                        i
                        for i in v.keys()
                        if i not in ["freq", "dtype", "dtype_sql", "is_suffix"]
                    ]
                )
                > 0
            ):
                raise TypeError(
                    "dct_dtype should be of form {col: {'freq': 'x', 'dtype': 'x'} }"
                )

    _dct_d, _dct_r = _get_dtype_toml()
    if dct_dtype is None:
        dct_dtype = _dct_d
    else:
        dct_dtype = _dct_d | dct_dtype

    if dct_regex is None:
        dct_regex = _dct_r
    else:
        dct_regex = _dct_r | dct_regex

    # sanity check regex patterns, if dct_dypte col matches pattern throw error
    dct_regex_p = {re.compile(p): v for p, v in dct_regex.items()}
    for c in list(dct_dtype.keys()):
        for p, v in dct_regex_p.items():
            if len([*p.splititer(c)]) != 1:
                raise KeyError(
                    f"dtype column {c} matches dtype regex pattern {p.pattern}, regex patterns cannot overlap with column names"
                )

    return dct_dtype, dct_regex


def _get_data_col_info(col, dct_dtypes, dct_regex) -> DataColumn:
    if col in dct_dtypes.keys():
        return DataColumn(col, **dct_dtypes[col])

    # compile regex patterns
    dct_regex_p = {re.compile(p): v for p, v in dct_regex.items()}

    # dct of regex cleaned column name, vvix_diff -> vvix, pq3_vvix -> vvix
    dct_col_regex = {
        "".join(p.split(col)): v | {"regex_p": p}
        for p, v in dct_regex_p.items()
        if len(p.split(col)) > 1
    }
    # lst = [p.split(col) for p in dct_regex_p.keys()]

    # when several regex apply pick non-suffix with priority, if singular use this one
    # use not suffix if exaclty one non-suffix pattern applies
    dct_col_regex_not_suffix = {
        k: d for k, d in dct_col_regex.items() if not d["is_suffix"]
    }

    if (len(dct_col_regex) > 1) and (len(dct_col_regex_not_suffix) != 1):
        MY_LOGGER.warning(f"{col} matches two regex patterns, defaulting to float, D")
        return DataColumn(col, "float", "D", is_default=True)
    elif len(dct_col_regex) == 0:
        MY_LOGGER.warning(f"{col} no info: defaulting to float, D")
        return DataColumn(col, "float", "D", is_default=True)
    else:
        # use dct_col_regex_suffix instead of dct_col_regex if several regex
        if (len(dct_col_regex) > 1) and (len(dct_col_regex_not_suffix) == 1):
            MY_LOGGER.info(
                f"{col} matches two regex patterns: {[i['regex_p'] for i in dct_col_regex.values()]}: USING {list(dct_col_regex_not_suffix.values())[0]['regex_p']}"
            )
            dct_col_regex = dct_col_regex_not_suffix

        # cleaned column name, regex dtype
        col_cleaned, dtype_regex = list(dct_col_regex.items())[0]

        # if regex cleaned col in dct_dtypes, then use its specification
        if col_cleaned in dct_dtypes.keys():
            # replace c dtypes with regex dtpyes if any
            d = dct_dtypes[col_cleaned] | {
                k: v for k, v in dtype_regex.items() if v != ""
            }
            return DataColumn(col, **d)

        elif dtype_regex["is_suffix"]:
            MY_LOGGER.warning(
                f"{col}, {col_cleaned} suffix recognition failed info: defaulting to float, D"
            )
            return DataColumn(col, "float", "D", is_default=True)

        # prefix recognition
        else:
            return DataColumn(col, **dtype_regex)


def _get_df_col_info(df: pd.DataFrame, dct_dtypes, dct_regex) -> list[DataColumn]:
    dct_dtypes, dct_regex = _get_dict_freq_dtype_update(dct_dtypes, dct_regex)
    return [_get_data_col_info(col, dct_dtypes, dct_regex) for col in df.columns]


def lst_df_sort_by_freq(
    lst_df, str_freq: str = "high", dct_dtype: dict = None, dct_regex: dict = None
) -> list:
    assert str_freq in ["high", "low"], f"{str_freq} must be 'high' or 'low'"

    if str_freq == "high":
        i = -1
    if str_freq == "low":
        i = 1

    lst_ord = [_get_lowest_freq(df, dct_dtype, dct_regex)[i] for df in lst_df]
    lst = list(map(list, zip(*[lst_df, lst_ord])))
    lst = [i[0] for i in sorted(lst, key=lambda x: x[1], reverse=True)]
    return lst


def _get_lowest_freq(df, dct_dtype, dct_regex):
    # dct_dtype, dct_regex = _get_dict_freq_dtype_update(dct_dtype, dct_regex)
    lst_cols = _get_df_col_info(df, dct_dtype, dct_regex)
    dct_dtypes_col = {}
    for dtype in lst_cols:
        if dtype.is_default:
            MY_LOGGER.warning(f"{dtype.name} not in dct_dypte: using freq D")

        dct_dtypes_col[dtype.name] = dtype

    # get lowest frequency
    dct = {v.freq: v.freq_ordinary for k, v in dct_dtypes_col.items()}
    lowest_freq = min(dct, key=dct.get)
    lowest_freq_ord = dct[lowest_freq]
    highest_freq = max(dct, key=dct.get)
    highest_freq_ord = dct[highest_freq]
    return lowest_freq, lowest_freq_ord, highest_freq, highest_freq_ord


def get_freq_adj_df(
    df: pd.DataFrame | pd.Series,
    dct_dtype: dict = None,
    dct_regex: dict = None,
    agg_obs: str = "first",
    return_timestamp_index: bool = True,
):
    assert df.index.dtype == "datetime64[ns]", "df does not have datetime index"
    is_ser = False
    if isinstance(df, pd.Series):
        is_ser = True
        df = pd.Series(df).to_frame()

    lowest_freq, _, _, _ = _get_lowest_freq(df, dct_dtype, dct_regex)

    if agg_obs == "first":
        d = df.groupby(df.index.to_period(lowest_freq)).first()
    elif agg_obs == "last":
        d = df.groupby(df.index.to_period(lowest_freq)).last()
    elif agg_obs == "mean":
        d = df.groupby(df.index.to_period(lowest_freq)).mean()
    elif agg_obs == "median":
        d = df.groupby(df.index.to_period(lowest_freq)).quantile(0.5)
    else:
        raise KeyError(f"{agg_obs} is an invalde aggegration method")

    if return_timestamp_index:
        d.index = d.index.to_timestamp()

    if is_ser:
        d = d[d.columns[0]]
    return d


def df_merge_dt_freq(
    df_left,
    df_right,
    dct_dtype: dict = None,
    dct_regex: dict = None,
    str_merge_how: str = "outer",
):
    """
    This function merges two dt indexed dataframes. If the frequencies agree the it performs a simple left or outer
    merge (for right merge swap dfs!).
    If the frequencies do not align this functions takes the left df's frequency as baseline. It then merges the right
    df according to the left frequency creating a frequency adjusted key.
    :param df_l:
    :param df_r:
    :param dct_dtype:
    :param dct_regex:
    :param str_merge_how:
    :return:
    """

    df_l = df_left.copy()
    df_r = df_right.copy()
    assert df_l.index.dtype == "datetime64[ns]", "df does not have datetime index"
    assert df_r.index.dtype == "datetime64[ns]", "df does not have datetime index"
    assert str_merge_how in [
        "outer",
        "left",
    ], f"function not defined for str_merge_how={str_merge_how}, for 'right' swap dfs!"

    lf_left, lf_left_ord, hf_left, hf_left_ord = _get_lowest_freq(
        df_l, dct_dtype, dct_regex
    )
    lf_right, lf_right_ord, hf_right, hf_right_ord = _get_lowest_freq(
        df_r, dct_dtype, dct_regex
    )

    if hf_left_ord > hf_right_ord:
        df_l["key"] = df_l.index.to_period(hf_right).to_timestamp()

        if str_merge_how == "outer":
            df = pd.merge(df_l, df_r, left_on="key", right_index=True, how="outer")
            name = df.index.name
            if name is None:
                name = "index"
            df = df.reset_index()
            df["calendardate"] = df[name].fillna(df["key"])
            df = df.set_index(
                "calendardate",
            )

        else:
            df = pd.merge(
                df_l, df_r, left_on="key", right_index=True, how=str_merge_how
            )

    elif hf_left_ord == hf_right_ord:
        df_l.index = df_l.index.to_period(hf_left)
        df_r.index = df_r.index.to_period(hf_right)
        df = df_l.join(df_r, how=str_merge_how)
        df.index = df.index.to_timestamp()

    else:
        df_r["key"] = df_r.index.to_period(hf_left).to_timestamp()
        df_r = df_r.sort_index().groupby("key").last()
        df = df_l.join(df_r, how=str_merge_how)

    for col in ["index", "key"]:
        if col in df.columns:
            df = df.drop(col, axis=1)

    df.index.name = "calendardate"
    df = df_cast_data(
        df.reset_index(), dct_dtype=dct_dtype, dct_regex=dct_regex
    ).set_index("calendardate")
    return df


def lst_df_merge_dt_freq(
    lst_df, how: str = "outer", dct_dtype: dict = None, dct_regex: dict = None
):
    df = ft.reduce(
        lambda left, right: df_merge_dt_freq(
            left, right, str_merge_how=how, dct_dtype=dct_dtype, dct_regex=dct_regex
        ),
        lst_df_sort_by_freq(lst_df, dct_dtype=dct_dtype, dct_regex=dct_regex),
    )
    return df


def df_cast_data(
    df, dct_dtype: dict = None, dct_regex: dict = None, is_cast_index: bool = False
) -> pd.DataFrame:
    df = df.copy()

    is_index_dropped, col_idx_name = False, None
    if is_cast_index:
        if (df.index.name != "") and (df.index.name is not None):
            is_index_dropped = True
            col_idx_name = df.index.name
            df = df.reset_index(drop=False)
        else:
            MY_LOGGER.warning("Index has no name and cannot be casted")

    for dtype in _get_df_col_info(df, dct_dtype, dct_regex):
        try:
            if dtype.dtype == "category":
                df[dtype.name] = pd.Categorical(df[dtype.name].astype(str))
            elif dtype.dtype == "categoryO":
                df[dtype.name] = pd.Categorical(df[dtype.name], ordered=True)
            else:
                df[dtype.name] = df[dtype.name].astype(dtype.dtype)

        except Exception as e:
            MY_LOGGER.warning(f"ERROR for {dtype.name} of type {dtype}: {e}")

    if is_index_dropped:
        df = df.set_index(col_idx_name)
    return df


def get_freq_sparse_df(
    df, sparse_kind: str = "first", dct_dtype: dict = None, dct_regex: dict = None
) -> pd.DataFrame:
    assert isinstance(df.index, pd.DatetimeIndex), "no datetime index present"
    assert sparse_kind in [
        "first",
        "last",
    ], 'sparse_king must be either "first" or "last"'

    # assign lowest frequency
    lf, lf_ord, hf, hf_ord = _get_lowest_freq(df, dct_dtype, dct_regex)
    df = df.asfreq(hf).sort_index()

    # make cols with freq > lf sparse
    for dtype in _get_df_col_info(df, dct_dtype, dct_regex):
        ser = df.groupby(df.index.to_period(dtype.freq).to_timestamp())[
            dtype.name
        ].first()
        if sparse_kind == "last":
            ser = df.groupby(df.index.to_period(dtype.freq).to_timestamp())[
                dtype.name
            ].last()

        df = df.drop(
            dtype.name,
            axis=1,
        ).join(ser, how="outer")

    return df.dropna(thresh=1)
