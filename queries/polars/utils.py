import pathlib
import tempfile
from functools import partial
from typing import Literal

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


def _preload_engine(
    engine: pl.GPUEngine | Literal["in-memory", "streaming", "old-streaming"],
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        # GPU engine has one-time lazy-loaded cost in IO, which we
        # remove from timings here.
        f = pathlib.Path(tmpdir) / "test.pq"
        df = pl.DataFrame({"a": [1]})
        df.write_parquet(f)
        pl.scan_parquet(f).collect(engine=engine)  # type: ignore[arg-type]


def obtain_engine_config() -> (
    pl.GPUEngine | Literal["in-memory", "streaming", "old-streaming"]
):
    if settings.run.polars_streaming:
        return "old-streaming"
    if settings.run.polars_new_streaming:
        return "streaming"
    if not settings.run.polars_gpu:
        return "in-memory"

    import cudf_polars
    import rmm
    from cudf_polars.callback import set_device
    from packaging import version

    if version.parse(cudf_polars.__version__) < version.Version("24.10"):
        import cudf._lib.pylibcudf as plc
    else:
        import pylibcudf as plc

    device = settings.run.polars_gpu_device
    mr_type = settings.run.use_rmm_mr
    with set_device(device):
        free_mem, _ = rmm.mr.available_device_memory()
        init_pool   = 256 * (int(free_mem * 0.9) // 256)

        if mr_type == "cuda":
            mr = rmm.mr.CudaMemoryResource()

        elif mr_type == "cuda-pool":
            mr = rmm.mr.PoolMemoryResource(
                    rmm.mr.CudaMemoryResource(),
                    initial_pool_size=init_pool
                )
        elif mr_type == "cuda-binning":
            mr = rmm.mr.BinningMemoryResource(
                rmm.mr.PoolMemoryResource(
                    rmm.mr.CudaMemoryResource(),
                    initial_pool_size=init_pool,
                ),
                min_size_exponent=16,
                max_size_exponent=24,
            )
        elif mr_type == "managed":
            mr = rmm.mr.ManagedMemoryResource()
        elif mr_type == "managed-pool" or mr_type == "managed-binning":
            if mr_type == "managed-pool":
                mr = rmm.mr.PrefetchResourceAdaptor(
                    rmm.mr.PoolMemoryResource(
                        rmm.mr.ManagedMemoryResource(),
                        initial_pool_size=init_pool
                    )
                )
            elif mr_type == "managed-binning":
                mr = rmm.mr.PrefetchResourceAdaptor(
                    rmm.mr.BinningMemoryResource(
                        rmm.mr.CudaMemoryResource(),
                        min_size_exponent=12,
                        max_size_exponent=26,
                    )
                )
            for typ in [
                "column_view::get_data",
                "mutable_column_view::get_data",
                "gather",
                "hash_join",
            ]:
                plc.experimental.enable_prefetching(typ)
        elif mr_type == "multi":
            # This will use all GPUs on the local host by default
            from dask_cuda import LocalCUDACluster
            from dask.distributed import Client, wait

            client = Client(LocalCUDACluster())
            executor_options = {"scheduler": "distributed"}  # Use "synchronous" for single GPU streaming execution
            executor = "streaming"

            return pl.GPUEngine(
                executor=executor,
                executor_options=executor_options,
                raise_on_fail=True,
            )

        else:
            raise RuntimeError(f"Unsupported MR profile: {mr_type!r}")

        return pl.GPUEngine(device=device, memory_resource=mr, raise_on_fail=True)


def run_query(query_number: int, lf: pl.LazyFrame) -> None:
    streaming = settings.run.polars_streaming
    new_streaming = settings.run.polars_new_streaming
    eager = settings.run.polars_eager
    gpu = settings.run.polars_gpu
    cloud = settings.run.polars_cloud

    if sum([eager, streaming, new_streaming, gpu, cloud]) > 1:
        msg = "Please specify at most one of eager, streaming, new_streaming, cloud or gpu"
        raise ValueError(msg)

    if eager:
        library_name = "polars-eager"
    elif gpu:
        library_name = f"polars-gpu-{settings.run.use_rmm_mr}"
    elif streaming:
        library_name = "polars-streaming"
    elif new_streaming:
        library_name = "polars-new-streaming"
    elif cloud:
        library_name = "polars-cloud"
    else:
        library_name = "polars"

    if settings.run.polars_show_plan:
        print(
            lf.explain(  # type: ignore[call-arg]
                streaming=streaming, new_streaming=new_streaming, optimized=eager
            )
        )

    engine = obtain_engine_config()
    if settings.run.polars_show_plan:
        print(lf.explain(engine=engine, optimized=not eager))  # type: ignore[arg-type]

    # Eager load engine backend, so we don't time that.
    _preload_engine(engine)

    if cloud:
        import os

        import polars_cloud as pc

        os.environ["POLARS_SKIP_CLIENT_CHECK"] = "1"

        class PatchedComputeContext(pc.ComputeContext):
            def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                self._interactive = True
                self._compute_address = "localhost:5051"
                self._compute_public_key = b""
                self._compute_id = "1"  # type: ignore[assignment]

            def get_status(self: pc.ComputeContext) -> pc.ComputeContextStatus:
                """Get the status of the compute cluster."""
                return pc.ComputeContextStatus.RUNNING

        pc.ComputeContext.__init__ = PatchedComputeContext.__init__  # type: ignore[assignment]
        pc.ComputeContext.get_status = PatchedComputeContext.get_status  # type: ignore[method-assign]

        def query():  # type: ignore[no-untyped-def]
            result = pc.spawn(
                lf, dst="file:///tmp/dst/", distributed=True
            ).await_result()

            if settings.run.show_results:
                print(result.plan())
            return result.lazy().collect()
    else:
        query = partial(
            lf.collect,
            no_optimization=eager,
            engine=engine,
        )

    try:
        run_query_generic(
            query,
            query_number,
            library_name,
            library_version=pl.__version__,
            query_checker=check_query_result_pl,
        )
    except Exception as e:
        print(f"q{query_number} FAILED\n{e}")
