# Performance Regression

This directory contains the framework for keeping track of performance
metrics.

# Prerequisites

In order to enable the performance regression scripts, the following
CMake variables must be set:

- CMAKE_BUILD_TYPE="Debug"
- CUMULATIVE_TIMES=On
- BENCHMARK=Off

Python 2.7 and gnuplot 5.2+ are required.
SQLite3 needs to be compiled with DECLTYPE turned on (do not define
SQLITE_OMIT_DECLTYPE).

# Initialization

In order to have a database of performance numbers, a database must be
created.  The `initialize.py` script will generate a SQLite database
file with tables containing columns appropriate for the requested
executable. These are specified by the executable specific
configuration script.

# Configurations

An executable may be run on multiple configurations. Some of the more
obvious variable configurations, such as CPU and memory information,
have been listed in `configurations.COLUMNS`, and can be set through
`configuration.py` command line arguments. These fields are all free
form. Each row will be unique and will generate a unique hash. This
hash will be used to identify the configuration in the other
scripts. The configuration script will run without adding the
configuration into the configuration table. Once you are satisfied
with the configuration, use the `--add` flag to add store the
configuration.

## Notes

The entire configuration hash does not have to be used when being
passed into other scripts. Only enough of the first few nibbles of the
hash that can uniquely identify the hash have to be kept.

# Running Trials and Collecting Raw Numbers

`run.py` will extract the arguments from the configuration table and
use them to run the executable. The executables must be findable by
the environment. The raw numbers will be collected from stderr and
placed into a temporary table. Statistics of the runs will be
generated in temporary views and printed out, but not stored. Use the
`--add` flag to move the raw numbers to the non-temporary table.  The
statistics will be automatically generated in the non-temporary views.
The executable should be run at least 2 times so that stddev does not
divide by 0.

# Comparing Statistics

The `compare.py` script takes in the database file name, two commit
hashes (which may be the same), the statistic to compare, and a series
of column names with ranges to compare the statistic with.

The format of the column names with ranges is:

``` name=float,float ```

where the new value is compared against an open interval created by
the two floats added to the old value:

``` (old + lower, old + higher) ```

The name, old value, new value, and change (`same`, `increased`,
`decreased`) will be printed to stdout.

## Available Statistics

Currently, the statistics available are:

- average
- max
- min
- median
- stddev

More will be added in the future.

# Plotting

The statistics tables can be dumped in a format suitable for plotting
with `dump.py`. Each line contains:

``` <commit hash order> <commit hash> <column name> <column value> ```

These lines should be dumped into a file that should be passed into
`plot.sh`. The script will generate a svg file with the statistics of
each commit. The raw numbers that are plotted are currently hardcoded.

# Example Usage

```
GUFI_SRC=...
DB="perf.db"
INDEX="index"
EXEC="gufi_query"
STAT="average"

# Create an index
# Make "${EXEC}" findable by the shell (add its dirname to PATH)

# The performance directory is not installed, so have to go to GUFI source
cd "${GUFI_SRC}/contrib/performance"

# Create the database of raw numbers
# One database can only store numbers for one executable
./initialize.py "${DB}" "${EXEC}"

# Store information about the environment and command line arguments
# and save the output hash to a variable
# The arguments after ${EXEC} are executable dependent
CONFIG_HASH=$(./configuration.py                     \
              --name "machine name"                  \
              --cpu "CPU"                            \
              --memory "DRAM"                        \
              --storage "Drive/Filesystem"           \
              "${EXEC}"                              \
              --threads 1                            \
              --summary "SELECT * FROM summary"      \
              --entries "SELECT * FROM entries"      \
              --buffer_size 1024 --terse "${INDEX}"  \
              --add "${DB}")

# Run the executable and fill up the database with performance metrics
# Run this as the GUFI repository is updated with new commits
# If dropping caches, will need sudo
./run.py --add "${DB}" "${CONFIG_HASH}" "${EXEC}"

# Dump the average of each column that was generated from running on the same configuration hash
./dump.py "${DB}" "${EXEC}" "${CONFIG_HASH}" "${STAT}" > "${EXEC}.${STAT}"

# Plot the statistics across commits to "${EXEC}.${STAT}.svg"
./plot.sh "${DB}" "${CONFIG_HASH}" "${EXEC}.${STAT}"
```

# How to add a new executable

Adding new executables should be fairly simple. Only a few variables
and functions needs to be created in an executable specific python
file:

- A list of `Column.Columns` listing possible executables arguments.

- A value specifying the name of the table that contains the raw
  values

- A list of `Column.Columns` listing the timings the are collected
  from the executables.

- A function that generates the necessary information for running the
  executable and collecting the timing information.

- A function that parses the raw output from the executable.

These variables and functions should be used to create a new
`available.ExecInfo` in `available.py`, which will allow for the other
scripts to process the new executable. For more details, see
`available.ExecInfo`.
