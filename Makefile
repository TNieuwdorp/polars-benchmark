# Default goal
.DEFAULT_GOAL := help

# Variables
PYTHONPATH    =
SHELL         = /bin/bash
.SHELLFLAGS   = -eu -o pipefail -c
VENV          = .venv
VENV_BIN      = $(VENV)/bin
SCALE_FACTOR ?= 1.0

# Phony targets
.PHONY: \
    run-10-times \
    install-deps \
    .venv \
    tables \
    run-all \
    run-all-polars \
    run-all-gpu \
    run-polars \
    run-polars-eager \
    run-polars-gpu \
    run-polars-streaming \
    run-polars-no-env \
    run-polars-gpu-no-env \
    run-fireducks \
    run-cudf \
    run-duckdb \
    run-pandas \
    run-pyspark \
    run-dask \
    run-modin \
    plot \
    clean \
    clean-tpch-dbgen \
    clean-tables \
    help

# Main target to run benchmarks 10 times
run-10-times: install-deps tables
	@if [ -z "$(HARDWARE)" ]; then \
		echo "Error: HARDWARE environment variable is not set."; \
		exit 1; \
	fi
	for i in {1..10}; do \
		for scale in 0.1 1.0 5.0 10.0 15.0 20.0 25.0 30.0; do \
			echo "Running benchmarks for SCALE_FACTOR=$$scale (iteration $$i)"; \
			$(MAKE) run-all SCALE_FACTOR=$$scale; \
			mv output/run/timings.csv output/run/timings-$(HARDWARE)-scale-$$scale-iteration-$$i.csv; \
		done; \
	done

# Set up Python virtual environment using uv
.venv:
	curl -LsSf https://astral.sh/uv/install.sh | sh
	export PATH=$$HOME/.cargo/bin:$$PATH
	uv venv --python 3.11 --seed

# Install Python project dependencies using uv
install-deps: .venv/.installed-deps

.venv/.installed-deps: .venv
	unset CONDA_PREFIX
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip install --compile -r requirements.txt --extra-index-url=https://pypi.nvidia.com
	touch $@

# Generate data tables if not already generated
tables: data/tables/scale-$(SCALE_FACTOR)/.done

data/tables/scale-%/.done: install-deps
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -vf -s $*
	mkdir -p "data/tables/scale-$*"
	mv tpch-dbgen/*.tbl "data/tables/scale-$*/"
	$(VENV_BIN)/python -m scripts.prepare_data
	rm -f "data/tables/scale-$*"/*.tbl
	touch $@

# Run all benchmarks
run-all: run-all-polars run-duckdb run-pandas run-pyspark run-dask run-modin

# Run all Polars benchmarks
run-all-polars: run-polars run-polars-eager run-polars-gpu run-polars-streaming

# Run all GPU-accelerated library benchmarks
run-all-gpu: run-polars run-polars-gpu run-pandas run-cudf

# Individual benchmark targets
run-polars: install-deps tables
	$(VENV_BIN)/python -m queries.polars

run-polars-eager: install-deps tables
	POLARS_EAGER=1 $(VENV_BIN)/python -m queries.polars

run-polars-gpu: install-deps tables
	POLARS_GPU=1 $(VENV_BIN)/python -m queries.polars

run-polars-streaming: install-deps tables
	POLARS_STREAMING=1 $(VENV_BIN)/python -m queries.polars

run-polars-no-env:
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -f -s $(SCALE_FACTOR)
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	python -m scripts.prepare_data
	rm -f data/tables/scale-$(SCALE_FACTOR)/*.tbl
	python -m queries.polars

run-polars-gpu-no-env: run-polars-no-env
	POLARS_GPU=1 CUDA_MODULE_LOADING=EAGER python -m queries.polars

run-fireducks: install-deps tables
	$(VENV_BIN)/python -m queries.fireducks

run-cudf: install-deps tables
	$(VENV_BIN)/python -m queries.cudf

run-duckdb: install-deps tables
	$(VENV_BIN)/python -m queries.duckdb

run-pandas: install-deps tables
	$(VENV_BIN)/python -m queries.pandas

run-pyspark: install-deps tables
	$(VENV_BIN)/python -m queries.pyspark

run-dask: install-deps tables
	$(VENV_BIN)/python -m queries.dask

run-modin: install-deps tables
	$(VENV_BIN)/python -m queries.modin

# Plot results
plot: install-deps
	$(VENV_BIN)/python -m scripts.plot_bars

# Clean up everything
clean: clean-tpch-dbgen clean-tables
	rm -rf .mypy_cache/ .venv/ output/ spark-warehouse/

# Clean up TPC-H folder
clean-tpch-dbgen:
	$(MAKE) -C tpch-dbgen clean
	rm -f tpch-dbgen/*.tbl

# Clean up data tables
clean-tables:
	rm -rf data/tables/

# Display help
help:
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-zA-Z0-9_\.-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort
