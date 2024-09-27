export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=1.0
export RUN_INCLUDE_IO=1

make tables
make run-all
make plot
