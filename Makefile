.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin

.PHONY: do
run-10-times: run-all
	@for i in {1..10}; do \
		$(MAKE) run-all; \
	done

.venv:  ## Set up Python virtual environment and install dependencies
	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv venv --python 3.13 --seed
	$(MAKE) install-deps

.PHONY: install-deps
install-deps: .venv  ## Install Python project dependencies
	@unset CONDA_PREFIX \
	&& $(VENV_BIN)/python -m pip install --upgrade uv \
	&& $(VENV_BIN)/uv pip install --compile -r requirements.txt \
	&& $(VENV_BIN)/uv pip install --compile -r requirements-dev.txt

.PHONY: bump-deps
bump-deps: .venv  ## Bump Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile requirements.in > requirements.txt
	$(VENV_BIN)/uv pip compile requirements-dev.in > requirements-dev.txt

.PHONY: install-gpu-env 
install-gpu-env:
	@if ! conda info --envs | grep -q "rapids-24.08"; then \
		echo "Conda environment 'rapids-24.08' not found. Installing..."; \
		conda create -n rapids-24.08 -c rapidsai -c conda-forge -c nvidia \
			cudf=24.08 python=3.11 'cuda-version>=12.0,<=12.5' --yes; \
		conda run -n rapids-24.08 pip install -r requirements-polars-gpu.txt; \
	else \
		echo "Conda environment 'rapids-24.08' already exists. Skipping installation."; \
	fi

.PHONY: fmt
fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

.PHONY: pre-commit
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

.PHONY: run-polars
run-polars: .venv tables  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.polars

.PHONY: run-fireducks
run-fireducks: .venv tables  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.fireducks

.PHONY: run-cudf
run-cudf: install-gpu-env tables  ## Run Polars benchmarks
	conda run -n rapids-24.08 python -m queries.cudf

.PHONY: run-polars-eager
run-polars-eager: .venv tables  ## Run Polars benchmarks
	POLARS_EAGER=1 $(VENV_BIN)/python -m queries.polars

.PHONY: run-polars-gpu
run-polars-gpu: install-gpu-env tables  ## Run Polars benchmarks
	POLARS_GPU=1 conda run -n rapids-24.08 python -m queries.polars

.PHONY: run-polars-streaming
run-polars-streaming: .venv tables  ## Run Polars benchmarks
	POLARS_STREAMING=1 $(VENV_BIN)/python -m queries.polars

.PHONY: run-polars-no-env
run-polars-no-env:  ## Run Polars benchmarks
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -f -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	python -m queries.polars

.PHONY: run-polars-gpu-no-env
run-polars-gpu-no-env: run-polars-no-env ## Run Polars CPU and GPU benchmarks
	RUN_POLARS_GPU=true CUDA_MODULE_LOADING=EAGER python -m queries.polars

.PHONY: run-duckdb tables
run-duckdb: .venv  ## Run DuckDB benchmarks
	$(VENV_BIN)/python -m queries.duckdb

.PHONY: run-pandas tables
run-pandas: .venv  ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas

.PHONY: run-pyspark tables
run-pyspark: .venv  ## Run PySpark benchmarks
	$(VENV_BIN)/python -m queries.pyspark

.PHONY: run-dask tables
run-dask: .venv  ## Run Dask benchmarks
	$(VENV_BIN)/python -m queries.dask

.PHONY: run-modin tables
run-modin: .venv  ## Run Modin benchmarks
	$(VENV_BIN)/python -m queries.modin

.PHONY: run-all
run-all: run-all-polars run-cudf run-fireducks run-duckdb run-pandas run-pyspark run-dask run-modin  ## Run all benchmarks

.PHONY: run-all-polars
run-all-polars: run-polars run-polars-eager run-polars-gpu run-polars-streaming ## Run all Polars benchmarks

.PHONY: run-all-gpu
run-all-gpu: run-polars run-polars-gpu run-pandas run-cudf  ## Run all GPU accelarated library benchmarks

.PHONY: plot
plot: .venv  ## Plot results
	$(VENV_BIN)/python -m scripts.plot_bars

.PHONY: clean
clean:  clean-tpch-dbgen clean-tables  ## Clean up everything
	$(VENV_BIN)/ruff clean
	@rm -rf .mypy_cache/
	@rm -rf .venv/
	@rm -rf output/
	@rm -rf spark-warehouse/

.PHONY: clean-tpch-dbgen
clean-tpch-dbgen:  ## Clean up TPC-H folder
	@$(MAKE) -C tpch-dbgen clean
	@rm -rf tpch-dbgen/*.tbl

.PHONY: clean-tables
clean-tables:  ## Clean up data tables
	@rm -rf data/tables/

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_0-9-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort
