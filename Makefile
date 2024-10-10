.DEFAULT_GOAL := help

# Variables
PYTHONPATH   =
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
	curl -LsSf https://astral.sh/uv/install.sh | sh
	PATH=$$HOME/.cargo/bin:$$PATH uv venv --python 3.11 --seed

install-deps: .venv .venv/.installed-deps  ## Install Python project dependencies if not already installed

.venv/.installed-deps: | .venv  ## Install only if dependencies aren't already installed
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/python -m pip install --upgrade uv \
	&& $(VENV_BIN)/uv pip install --compile -r requirements.txt --extra-index-url=https://pypi.nvidia.com \
	&& touch $@

bump-deps: .venv  ## Bump Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile requirements.in -o requirements.txt
	rm -f .venv/.installed-deps

bump-deps-with-gpu: .venv  ## Bump Python project dependencies with GPU support
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile --extra-index-url=https://pypi.nvidia.com requirements.in requirements-gpu.in -o requirements.txt
	rm -f .venv/.installed-deps

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
	$(VENV_BIN)/python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	touch $@

## Benchmark Runs

run-polars: install-deps tables  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.polars

run-fireducks: install-deps tables  ## Run Fireducks benchmarks
	$(VENV_BIN)/python -m queries.fireducks

run-cudf: install-deps tables  ## Run cuDF benchmarks
	$(VENV_BIN)/python -m queries.cudf

run-polars-eager: install-deps tables  ## Run Polars benchmarks in eager mode
	POLARS_EAGER=1 $(VENV_BIN)/python -m queries.polars

run-polars-gpu: install-deps tables  ## Run Polars GPU benchmarks
	POLARS_GPU=1 $(VENV_BIN)/python -m queries.polars

run-polars-streaming: install-deps tables  ## Run Polars streaming benchmarks
	POLARS_STREAMING=1 $(VENV_BIN)/python -m queries.polars

run-polars-no-env:  ## Run Polars benchmarks without virtual environment
	$(MAKE) -C tpch-dbgen dbgen
	(cd tpch-dbgen && ./dbgen -f -s $(SCALE_FACTOR))
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	python -m queries.polars

run-polars-gpu-no-env: run-polars-no-env  ## Run Polars CPU and GPU benchmarks without virtual environment
	RUN_POLARS_GPU=true CUDA_MODULE_LOADING=EAGER python -m queries.polars

run-duckdb: install-deps tables  ## Run DuckDB benchmarks
	$(VENV_BIN)/python -m queries.duckdb

run-pandas: install-deps tables  ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas

run-pyspark: install-deps tables  ## Run PySpark benchmarks
	$(VENV_BIN)/python -m queries.pyspark

run-dask: install-deps tables  ## Run Dask benchmarks
	$(VENV_BIN)/python -m queries.dask

run-modin: install-deps tables  ## Run Modin benchmarks
	$(VENV_BIN)/python -m queries.modin

## Run-all Targets

run-all: run-all-polars run-duckdb run-pandas run-pyspark run-dask run-modin  ## Run all benchmarks

run-all-polars: run-polars run-polars-eager run-polars-gpu run-polars-streaming  ## Run all Polars benchmarks

run-all-gpu: run-polars run-polars-gpu run-pandas run-cudf  ## Run all GPU-accelerated library benchmarks

## Plotting

plot: install-deps  ## Plot results
	$(VENV_BIN)/python -m scripts.plot_bars

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

run-10-times:  ## Run benchmarks 10 times over multiple SCALE_FACTOR values
	@if [ -z "$(HARDWARE)" ]; then \
		echo "Error: HARDWARE environment variable is not set."; \
		exit 1; \
	fi; \
	for i in {1..10}; do \
		for scale in 0.1 1.0 5.0 10.0 15.0 20.0 25.0 30.0; do \
			echo "Running benchmarks for SCALE_FACTOR=$$scale (iteration $$i)"; \
			SCALE_FACTOR=$$scale $(MAKE) run-all; \
			mv output/run/timings.csv output/run/timings-$(HARDWARE)-scale-$$scale-iteration-$$i.csv; \
		done; \
	done; \
	unset HARDWARE
