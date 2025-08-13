import gc
import os
import time
from itertools import chain

import numpy as np
from sqlalchemy.pool import NullPool
import mysql.connector
import pandas as pd
import sqlalchemy

from .helpers_data import (
    _get_dict_freq_dtype_update,
    _get_data_col_info,
    df_cast_data,
)
from .helpers_logging import get_logger

MY_LOGGER = get_logger(os.path.basename(__file__), stream_level=20)


def _get_sql_user_pw():
    if "SQL_DB_USER" not in os.environ.keys():
        MY_LOGGER.error(
            "please add SQL_DB_USER to os.environ or specify both 'user' and 'pw' in get_sql_connection"
        )
    if "SQL_DB_PW" not in os.environ.keys():
        MY_LOGGER.error(
            "please add SQL_DB_PW to os.environ or specify both 'user' and 'pw' in get_sql_connection"
        )

    user, pw = os.environ["SQL_DB_USER"], os.environ["SQL_DB_PW"]
    return user, pw


def get_sql_connection(
    database: str,
    user: str = None,
    pw: str = None,
    is_parallel: bool = False,
):
    """
    Returns MySQL database connection and cursor
    :param database:
    :return:
    """

    if (user is None) or (pw is None):
        user, pw = _get_sql_user_pw()

    global mySQLconnection, cursor, sql_engine
    if is_parallel:
        try:
            sql_engine = sqlalchemy.create_engine(
                f"mysql+mysqlconnector://{user}:{pw}@localhost/{database}",
                poolclass=NullPool,
            )
            sql_engine_con = sql_engine.connect()
        except Exception:
            MY_LOGGER.error("MYSQL connection error, attempting reconnect")
            time.sleep(20)
            sql_engine = sqlalchemy.create_engine(
                f"mysql+mysqlconnector://{user}:{pw}@localhost/{database}",
                poolclass=NullPool,
            )
            sql_engine_con = sql_engine.connect()

        mySQLconnection, cursor = None, None

    else:
        sql_engine = sqlalchemy.create_engine(
            f"mysql+mysqlconnector://{user}:{pw}@localhost/{database}"
        )
        sql_engine_con = sql_engine.connect()

        mySQLconnection = mysql.connector.connect(
            host="localhost",
            database=database,
            user=user,
            password=pw,
        )
        cursor = mySQLconnection.cursor()

    return mySQLconnection, cursor, sql_engine, sql_engine_con


def get_sql_tab_from_df(
    tab_name: str | None,
    db_name: str,
    df: pd.DataFrame = None,
    lst_cols: list = None,
    lst_index: list = None,
    auto_increment_index: str = None,
    dct_dtype: dict = None,
    dct_regex: dict = None,
    is_drop_table: bool = True,
):
    assert (lst_cols is None) ^ (df is None), "specify EITHER lst_cols OR df"

    if tab_name is None:
        MY_LOGGER.info("no table created for tab name None")
        return None

    sql_con, cursor, _, _ = get_sql_connection(database=db_name)
    dct_dtype, dct_regex = _get_dict_freq_dtype_update(dct_dtype, dct_regex)

    lst_tup_index_dtypes, lst_tup_index_other = None, None
    if df is not None:
        lst_cols = list(df.columns)

    if lst_index is None:
        # account for multi-index names
        if (df.index.name is not None) or (
            df.index.names != [None] * len(df.index.names)
        ):
            lst_tup_index_dtypes = [
                (i, _get_data_col_info(i, dct_dtype, dct_regex).dtype_sql)
                for i in df.index.names
            ]
        else:
            MY_LOGGER.warning(
                f"{tab_name}: df index does not have any names, using auto increment index"
            )
            lst_tup_index_dtypes = [("idx", "int NOT NULL AUTO_INCREMENT")]

    elif lst_index is not None:
        # multiple indexes
        if isinstance(lst_index[0], tuple):
            # lst of additional indixes, they should be contained in the table
            lst_tup_index_other = lst_index[1:]
            lst_index = list(lst_index[0])

            # check if other indices are present in table
            # if not present, add index to table
            lst = [*chain(*[list(i) for i in lst_tup_index_other])]
            for i in lst:
                if i not in [*chain(*[lst_index, lst_cols])]:
                    # lst_tup_index_other = [
                    #     tpl for tpl in lst_tup_index_other if i not in tpl
                    # ]
                    MY_LOGGER.debug(
                        f"{i} is an additional index but not contained in table {tab_name}, adding {i} to table"
                    )

                    # adding additional index to table columns, otherwise it cannot be added as an index later on
                    lst_cols.append(i)

        # get lst_tup_index for first index
        lst_tup_index_dtypes = [
            (i, _get_data_col_info(i, dct_dtype, dct_regex).dtype_sql)
            for i in lst_index
        ]

    # exclude index cols, avoiding double counting
    lst = [c for c in lst_cols if c in lst_tup_index_dtypes]
    if len(lst) > 0:
        MY_LOGGER.warning(
            f'following columns are both in index and columns: {", ".join(lst)}'
        )

    lst_cols = [c for c in lst_cols if c not in [i[0] for i in lst_tup_index_dtypes]]
    lst_col_dtypes = [_get_data_col_info(col, dct_dtype, dct_regex) for col in lst_cols]

    if auto_increment_index is not None:
        lst_tup_index_dtypes += [(auto_increment_index, "int NOT NULL AUTO_INCREMENT")]

    if is_drop_table:
        cursor.execute(f"drop table if exists {db_name}.{tab_name}")

    str_sql_index = ",\n".join([" ".join(t) for t in lst_tup_index_dtypes])
    str_sql_cols = ",\n".join(
        [c.name + f" {c.dtype_sql} default null" for c in lst_col_dtypes]
    )
    cursor.execute(f"""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables
        WHERE TABLE_SCHEMA = '{db_name}'
        AND TABLE_NAME = '{tab_name}'
    """)
    is_tab_exist = [*chain(*[*cursor])] == [tab_name]

    str_sql_req = f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{tab_name} (
                    {str_sql_index},
                    {str_sql_cols},
              KEY `Index_1` ( {', '.join([t[0] for t in lst_tup_index_dtypes])} ) USING BTREE
              )
              ENGINE=MyISAM DEFAULT CHARSET=utf8mb4
        """
    cursor.execute(str_sql_req)
    sql_con.commit()

    # add other indices to table
    if (lst_tup_index_other is not None) and len(lst_tup_index_other) > 0:
        if not is_drop_table:
            MY_LOGGER.info(
                "is_drop_table set to FALSE, adding additional indices may take longer"
            )
        lst = [
            f"ALTER TABLE {tab_name} ADD INDEX idx{i+1} ({", ".join(tpl)})"
            for i, tpl in enumerate(lst_tup_index_other)
        ]
        for s in lst:
            cursor.execute(s)
        sql_con.commit()

    if not is_tab_exist:
        MY_LOGGER.info(f"created: {db_name}.{tab_name}")
    elif is_tab_exist and (not is_drop_table):
        MY_LOGGER.info(f"tab existed, not overwritten: {tab_name}")

    pass


def write_df_to_sql(
    df: pd.DataFrame,
    tab_name: str,
    sql_eng,
    if_exists: str = "append",
    is_index: bool = False,
    dct_dtype: dict = None,
    dct_regex: dict = None,
    chunksize: int = 500,
):
    if (tab_name is None) or (df is None) or df.empty:
        return None

    db_name = str(sql_eng.engine.url).split("/")[-1]
    lst_sql_col_names = list(
        pd.read_sql(
            f"""
                            SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
                            WHERE `TABLE_SCHEMA`='{db_name}'
                            AND `TABLE_NAME`='{tab_name}'
                                        """,
            sql_eng,
        ).values.ravel()
    )

    df = df_cast_data(df, dct_dtype=dct_dtype, dct_regex=dct_regex)
    for col in df.columns:
        if df[col].dtype == float:
            df[col] = df[col].replace({np.inf: np.nan, -np.inf: np.nan})

    lst_df_cols_not_in_table = [
        col for col in df.columns if col not in lst_sql_col_names
    ]
    if len(lst_df_cols_not_in_table) > 0:
        MY_LOGGER.warning(
            f'Dropped following columns which are not in SQL table {tab_name}: {", ".join(lst_df_cols_not_in_table)}'
        )
        df = df.drop(lst_sql_col_names, axis=1)

    df.to_sql(
        name=tab_name,
        con=sql_eng,
        if_exists=if_exists,
        index=is_index,
        chunksize=chunksize,
    )
    del df
    gc.collect()
    pass
