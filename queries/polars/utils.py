from functools import partial

import polars as pl

from queries.common_utils import (
    check_query_result_pl,
    get_table_path,
    run_query_generic,
)
from settings import Settings

settings = Settings()


def _scan_ds(table_name: str) -> pl.LazyFrame:
    path = get_table_path(table_name)

    if settings.run.io_type == "skip":
        return pl.read_parquet(path, rechunk=True).lazy()
    if settings.run.io_type == "parquet":
        return pl.scan_parquet(path)
    elif settings.run.io_type == "feather":
        return pl.scan_ipc(path)
    elif settings.run.io_type == "csv":
        return pl.scan_csv(path, try_parse_dates=True)
    else:
        msg = f"unsupported file type: {settings.run.io_type!r}"
        raise ValueError(msg)


def get_line_item_ds() -> pl.LazyFrame:
    return _scan_ds("lineitem")


def get_orders_ds() -> pl.LazyFrame:
    return _scan_ds("orders")


def get_customer_ds() -> pl.LazyFrame:
    return _scan_ds("customer")


def get_region_ds() -> pl.LazyFrame:
    return _scan_ds("region")


def get_nation_ds() -> pl.LazyFrame:
    return _scan_ds("nation")


def get_supplier_ds() -> pl.LazyFrame:
    return _scan_ds("supplier")


def get_part_ds() -> pl.LazyFrame:
    return _scan_ds("part")


def get_part_supp_ds() -> pl.LazyFrame:
    return _scan_ds("partsupp")


def run_query(query_number: int, lf: pl.LazyFrame) -> None:
    streaming = settings.run.polars_streaming
    eager = settings.run.polars_eager
    engine = "gpu" if settings.run.polars_gpu else "cpu"
    streaming = settings.run.polars_streaming

    if eager:
        library_name = "polars-eager"
    elif engine == "gpu": 
        engine = pl.lazyframe.engine_config.GPUEngine(raise_on_fail=True)
        library_name = "polars-gpu"
    elif streaming:
        library_name = "polars-streaming"
    else:
        library_name = "polars"

    if settings.run.polars_show_plan:
        print(lf.explain(streaming=streaming, optimized=eager))

    query = partial(lf.collect, streaming=streaming, no_optimization=eager, engine=engine)


    run_query_generic(
        query,
        query_number,
        library_name,
        library_version=pl.__version__,
        query_checker=check_query_result_pl,
    )
