.DEFAULT_GOAL := help

PYTHONPATH =
SHELL       = /bin/bash
VENV        = .venv
VENV_BIN    = $(VENV)/bin
SCALE_FACTOR ?= 1

.PHONY: \
    run-10-times \
    install-deps \
    bump-deps \
    install-gpu-env \
    fmt \
    pre-commit \
    tables \
    run-polars \
    run-fireducks \
    run-cudf \
    run-polars-eager \
    run-polars-gpu \
    run-polars-streaming \
    run-polars-no-env \
    run-polars-gpu-no-env \
    run-duckdb \
    run-pandas \
    run-pyspark \
    run-dask \
    run-modin \
    run-all \
    run-all-polars \
    run-all-gpu \
    plot \
    clean \
    clean-tpch-dbgen \
    clean-tables \
    help

run-10-times:
	@for i in {1..10}; do \
		$(MAKE) run-all; \
	done

.venv:  ## Set up Python virtual environment
	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv venv --python 3.13 --seed

install-deps: .venv  ## Install Python project dependencies
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/python -m pip install --upgrade uv \
	&& $(VENV_BIN)/uv pip install --compile -r requirements.txt \
	&& $(VENV_BIN)/uv pip install --compile -r requirements-dev.txt

bump-deps: .venv  ## Bump Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile requirements.in > requirements.txt
	$(VENV_BIN)/uv pip compile requirements-dev.in > requirements-dev.txt

install-gpu-env:
	@if ! conda info --envs | grep -q "rapids-24.08"; then \
		echo "Conda environment 'rapids-24.08' not found. Installing..."; \
		conda create -n rapids-24.08 -c rapidsai -c conda-forge -c nvidia \
			cudf=24.08 python=3.11 'cuda-version>=12.0,<=12.5' --yes; \
		conda run -n rapids-24.08 pip install -r requirements-polars-gpu.txt; \
	else \
		echo "Conda environment 'rapids-24.08' already exists. Skipping installation."; \
	fi

fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

pre-commit: fmt  ## Run all code quality checks

tables: data/tables/scale-$(SCALE_FACTOR)/.done  ## Alias for the dataset generation

data/tables/scale-$(SCALE_FACTOR)/.done: .venv  ## Generate data tables if not already generated
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	$(VENV_BIN)/python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	touch data/tables/scale-$(SCALE_FACTOR)/.done

run-polars: .venv tables  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.polars

run-fireducks: .venv tables  ## Run Fireducks benchmarks
	$(VENV_BIN)/python -m queries.fireducks

run-cudf: install-gpu-env tables  ## Run cuDF benchmarks
	conda run -n rapids-24.08 python -m queries.cudf

run-polars-eager: .venv tables  ## Run Polars benchmarks in eager mode
	POLARS_EAGER=1 $(VENV_BIN)/python -m queries.polars

run-polars-gpu: install-gpu-env tables  ## Run Polars GPU benchmarks
	POLARS_GPU=1 conda run -n rapids-24.08 python -m queries.polars

run-polars-streaming: .venv tables  ## Run Polars streaming benchmarks
	POLARS_STREAMING=1 $(VENV_BIN)/python -m queries.polars

run-polars-no-env:  ## Run Polars benchmarks without virtual environment
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -f -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	python -m queries.polars

run-polars-gpu-no-env: run-polars-no-env  ## Run Polars CPU and GPU benchmarks without virtual environment
	RUN_POLARS_GPU=true CUDA_MODULE_LOADING=EAGER python -m queries.polars

run-duckdb: .venv tables  ## Run DuckDB benchmarks
	$(VENV_BIN)/python -m queries.duckdb

run-pandas: .venv tables  ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas

run-pyspark: .venv tables  ## Run PySpark benchmarks
	$(VENV_BIN)/python -m queries.pyspark

run-dask: .venv tables  ## Run Dask benchmarks
	$(VENV_BIN)/python -m queries.dask

run-modin: .venv tables  ## Run Modin benchmarks
	$(VENV_BIN)/python -m queries.modin

run-all: run-all-polars run-cudf run-fireducks run-duckdb run-pandas run-pyspark run-dask run-modin  ## Run all benchmarks

run-all-polars: run-polars run-polars-eager run-polars-gpu run-polars-streaming  ## Run all Polars benchmarks

run-all-gpu: run-polars run-polars-gpu run-pandas run-cudf  ## Run all GPU-accelerated library benchmarks

plot: .venv  ## Plot results
	$(VENV_BIN)/python -m scripts.plot_bars

clean: clean-tpch-dbgen clean-tables  ## Clean up everything
	@rm -rf .mypy_cache/
	@rm -rf .venv/
	@rm -rf output/
	@rm -rf spark-warehouse/

clean-tpch-dbgen:  ## Clean up TPC-H folder
	@$(MAKE) -C tpch-dbgen clean
	@rm -rf tpch-dbgen/*.tbl

clean-tables:  ## Clean up data tables
	@rm -rf data/tables/

help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z0-9_\.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort
