from __future__ import annotations

from typing import TYPE_CHECKING, Any

import cudf.pandas
import cudf
import rmm

cudf.pandas.install()
import pandas as pd

from queries.common_utils import (
    check_query_result_cudf,
    get_table_path,
    on_second_call,
    run_query_generic,
)
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable

settings = Settings()

pd.options.mode.copy_on_write = True


def _read_ds(table_name: str) -> pd.DataFrame:
    path = get_table_path(table_name)
    if settings.run.io_type in ("parquet", "skip"):
        return cudf.read_parquet(path)
    elif settings.run.io_type == "csv":
        df = cudf.read_csv(path)
        # TODO: This is slow - we should use the known schema to read dates directly
        for c in df.columns:
            if c.endswith("date"):
                df[c] = df[c].astype("date32[day][pyarrow]")  # type: ignore[call-overload]
        return df
    elif settings.run.io_type == "feather":
        return cudf.read_feather(path)
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)


@on_second_call
def get_line_item_ds() -> pd.DataFrame:
    return _read_ds("lineitem")


@on_second_call
def get_orders_ds() -> pd.DataFrame:
    return _read_ds("orders")


@on_second_call
def get_customer_ds() -> pd.DataFrame:
    return _read_ds("customer")


@on_second_call
def get_region_ds() -> pd.DataFrame:
    return _read_ds("region")


@on_second_call
def get_nation_ds() -> pd.DataFrame:
    return _read_ds("nation")


@on_second_call
def get_supplier_ds() -> pd.DataFrame:
    return _read_ds("supplier")


@on_second_call
def get_part_ds() -> pd.DataFrame:
    return _read_ds("part")


@on_second_call
def get_part_supp_ds() -> pd.DataFrame:
    return _read_ds("partsupp")


def run_query(query_number: int, query: Callable[..., Any]) -> None:
    run_query_generic(
        query, query_number, library_name="cudf", library_version=cudf.__version__, query_checker=check_query_result_cudf
    )
