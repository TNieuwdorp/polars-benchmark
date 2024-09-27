.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin

.venv:  ## Set up Python virtual environment and install dependencies
	python3 -m venv $(VENV)
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
	conda create -n rapids-24.08 -c rapidsai -c conda-forge -c nvidia  \
		cudf=24.08 python=3.11 'cuda-version>=12.0,<=12.5'

.PHONY: fmt
fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

.PHONY: pre-commit
pre-commit: fmt  ## Run all code quality checks

data/tables: .venv  ## Generate data tables
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	$(VENV_BIN)/python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl
	touch data/tables/scale-$(SCALE_FACTOR)/.done

.PHONY: run-polars
run-polars: .venv data/tables/.done  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.polars

.PHONY: run-fireducks
run-fireducks: .venv data/tables/.done  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.fireducks

.PHONY: run-cudf
run-cudf: .venv data/tables/.done  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.cudf

.PHONY: run-polars-eager
run-polars-eager: .venv data/tables/.done  ## Run Polars benchmarks
	POLARS_EAGER=1 $(VENV_BIN)/python -m queries.polars

.PHONY: run-polars-gpu
run-polars-gpu: .venv data/tables/.done  ## Run Polars benchmarks
	POLARS_GPU=1 $(VENV_BIN)/python -m queries.polars

.PHONY: run-polars-streaming
run-polars-streaming: .venv data/tables/.done  ## Run Polars benchmarks
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

.PHONY: run-duckdb data/tables/.done
run-duckdb: .venv  ## Run DuckDB benchmarks
	$(VENV_BIN)/python -m queries.duckdb

.PHONY: run-pandas data/tables/.done
run-pandas: .venv  ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas

.PHONY: run-pyspark data/tables/.done
run-pyspark: .venv  ## Run PySpark benchmarks
	$(VENV_BIN)/python -m queries.pyspark

.PHONY: run-dask data/tables/.done
run-dask: .venv  ## Run Dask benchmarks
	$(VENV_BIN)/python -m queries.dask

.PHONY: run-modin data/tables/.done
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
