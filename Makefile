export PATH := $(HOME)/.local/bin:$(PATH)
UV_EXECUTABLE := $(HOME)/.local/bin/uv
.DEFAULT_GOAL := help

# Variables
SHELL        = /bin/bash
VENV         = .venv
VENV_BIN     = $(VENV)/bin
SCALE_FACTOR ?= 1.0

# Phony targets
.PHONY: \
    run-10-times \
    bump-deps \
    fmt \
    pre-commit \
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

## Setup and Installation

.venv:  ## Set up Python virtual environment
	mkdir -p "$(dir $(UV_EXECUTABLE))" && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$(dir $(UV_EXECUTABLE))" INSTALLER_NO_MODIFY_PATH=1 sh
	$(UV_EXECUTABLE) venv --python 3.12 --seed

install-deps: .venv .venv/.installed-deps  ## Install Python project dependencies if not already installed

.venv/.installed-deps: | .venv  ## Install only if dependencies aren't already installed
	@unset CONDA_PREFIX; uv sync; touch $@

## Code Quality and Formatting

fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

pre-commit: fmt  ## Run all code quality checks

## Data Preparation

tables: data/tables/scale-$(SCALE_FACTOR)/.done   ## Alias for the dataset generation

data/tables/scale-$(SCALE_FACTOR)/.done: | install-deps  ## Generate data tables if not already generated
	$(MAKE) -C tpch-dbgen dbgen
	(cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR))
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	uv run --with polars -m scripts.prepare_data --num-parts=1 --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	touch $@

data/tables/: data/tables/.generated
	@true

data/tables/partitioned/: .venv  ## Generate partitioned data tables (these are not yet runnable with current repo)
	$(MAKE) -C tpch-dbgen dbgen
	uv run --with polars -m scripts.prepare_data --num-parts=10 --tpch_gen_folder="data/tables/scale-$(SCALE_FACTOR)"

## Benchmark Runs
run-polars: install-deps tables  ## Run Polars benchmarks
	uv run --with polars -m queries.polars

run-fireducks: install-deps tables  ## Run Fireducks benchmarks
	uv run --with fireducks -m queries.fireducks

run-cudf: install-deps tables  ## Run cuDF benchmarks
	uv run --with cudf-cu12 -m queries.cudf --extra-index-url=https://pypi.nvidia.com

run-polars-eager: install-deps tables  ## Run Polars benchmarks in eager mode
	POLARS_EAGER=1 uv run --with polars -m queries.polars

run-polars-gpu: install-deps tables  ## Run Polars GPU benchmarks
	POLARS_GPU=1 CUDA_MODULE_LOADING=EAGER uv run --with polars[gpu] --with dask_cuda -m queries.polars

run-polars-streaming: install-deps tables  ## Run Polars streaming benchmarks
	POLARS_STREAMING=1 uv run --with polars -m queries.polars

run-polars-new-streaming: install-deps tables  ## Run Polars streaming benchmarks
	POLARS_NEW_STREAMING=1 uv run --with polars -m queries.polars

.PHONY: run-polars-no-env
run-polars-no-env: tables ## Run Polars benchmarks
	$(MAKE) -C tpch-dbgen dbgen
	(cd tpch-dbgen && ./dbgen -f -s $(SCALE_FACTOR))
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	python -m queries.polars

.PHONY: run-polars-gpu-no-env
run-polars-gpu-no-env: run-polars-no-env tables  ## Run Polars CPU and GPU benchmarks without virtual environment
	RUN_POLARS_GPU=true CUDA_MODULE_LOADING=EAGER python -m queries.polars

run-duckdb: install-deps tables  ## Run DuckDB benchmarks
	uv run --with duckdb --with polars --with pyarrow -m queries.duckdb

run-pandas: install-deps tables  ## Run pandas benchmarks
	uv run --with pandas --with pyarrow --with fastparquet -m queries.pandas

run-pyspark: install-deps tables  ## Run PySpark benchmarks
	uv run --with pyspark[pandas_on_spark] -m queries.pyspark

run-dask: install-deps tables  ## Run Dask benchmarks
	uv run --with dask[dataframe] -m queries.dask

run-modin: install-deps tables  ## Run Modin benchmarks
	uv run --with modin[ray] -m queries.modin

## Run-all Targets

run-all: run-all-polars run-cudf run-duckdb run-pandas run-pyspark run-dask #run-modin   ## Run all benchmarks

run-performant: run-polars run-polars-gpu run-cudf run-duckdb  ## Run all benchmarks for high scale datasets

run-all-polars: run-polars run-polars-eager run-polars-gpu run-polars-streaming run-polars-new-streaming ## Run all Polars benchmarks

run-all-gpu: run-polars-gpu run-cudf  ## Run all GPU-accelerated library benchmarks

## Plotting

plot: install-deps  ## Plot results
	uv run --with plotly -m scripts.plot_bars

## Cleaning

clean: clean-tpch-dbgen clean-tables  ## Clean up everything
	@rm -rf .mypy_cache/
	@rm -rf $(VENV)/
	@rm -rf output/
	@rm -rf spark-warehouse/

clean-tpch-dbgen:  ## Clean up TPC-H folder
	@$(MAKE) -C tpch-dbgen clean
	@rm -rf tpch-dbgen/*.tbl

clean-tables:  ## Clean up data tables
	@rm -rf data/tables/

## Help

help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z0-9_\.-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort

## Other

run-10-times-light:  ## Run benchmarks 10 times over multiple SCALE_FACTOR values
	for i in {1..5}; do \
		for scale in 0.1 1.0 2.5 5.0 10.0; do \
			echo "Running benchmarks for SCALE_FACTOR=$$scale (iteration $$i)"; \
			SCALE_FACTOR=$$scale $(MAKE) run-all; \
			mv output/run/timings.csv output/run/timings-$(HARDWARE)-scale-$$scale-iteration-$$i.csv; \
		done; \
	done;

run-10-times-heavy:
	@if [ -z "$(HARDWARE)" ]; then \
		echo "Error: HARDWARE environment variable is not set."; \
		exit 1; \
	fi; \
	for i in {1..5}; do \
		for scale in 25.0 50.0 75.0 100.0; do \
			echo "Running benchmarks for SCALE_FACTOR=$$scale (iteration $$i)"; \
			SCALE_FACTOR=$$scale $(MAKE) run-performant; \
			mv output/run/timings.csv output/run/timings-$(HARDWARE)-scale-$$scale-iteration-$$i.csv; \
		done; \
	done; \


benchmark:
	@if [ -z "$(HARDWARE)" ]; then \
		echo "Error: HARDWARE environment variable is not set."; \
		exit 1; \
	fi; \
	$(MAKE) run-10-times-light
	$(MAKE) run-10-times-heavy
	unset HARDWARE


multi-gpu-benchmark:
	@for i in {1..8}; do \
		for number_of_gpus in $$(seq 1 $$(nvidia-smi --query-gpu=count --format=csv,noheader | head -n 1)); do \
			for scale in 10.0 100.0; do \
				echo "Iteration $$i, number of gpus $$number_of_gpus, scale $$scale"; \
				SCALE_FACTOR=$$scale NUMBER_OF_GPUS=$$number_of_gpus POLARS_GPU=1 POLARS_GPU_PROFILE=multi $(MAKE) run-polars-gpu; \
			done; \
		done; \
	done;
