# Performance Regression

This directory contains the framework for keeping track of performance
metrics.

# Usage

## Prerequisites

In order to enable the performance regression scripts, the following
CMake variables must be set:

- CMAKE_BUILD_TYPE="Debug"
- CUMULATIVE_TIMES=On
- BENCHMARK=Off

Additionally, Python 2.7 and gnuplot 5.2+ are required.

## Initialization

In order to have a database of performance numbers, a database must be
created.  The `initialize.py` script will generate a SQLite database
file with tables containing columns appropriate for the requested
executable. These are specified by the executable specific
configuration script.

## Configurations

An executable may be run on multiple configurations. Some of the more
obvious variable configurations, such as CPU and memory information,
have been listed at the top of `configurations.COLUMNS`, and can be
set through `configuration.py` command line arguments. These fields
are all free form. Each row will be unique and will generate a unique
hash. This hash will be used to identify the configuration in the
other scripts. The configuration script will run without adding the
configuration into the configuration table. Once you are satisfied
with the configuration, use the `--add` flag to add store the
configuration.

### Notes

The entire configuration hash does not have to be used when being
passed into other scripts. Only enough of the first few nibbles of the
hash that can uniquely identify the hash have to be kept.

## Running Trials

`run.py` will extract the arguments from the configuration table and
use them to run the executable. Statistics of the run will be printed
out, but not stored by default. Use the `--add` flag to store them.

## Comparing Statistics

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

### Available Statistics

Currently, the statistics available are: `average`, `max`, and
`min`. More will be added in the future.

## Plotting

The statistics tables can be dumped in a format suitable for plotting
with `dump.py`. Each line contains:

``` <commit hash order> <commit hash> <column name> <column value> ```

These lines should be dumped into a file that should be passed into
`plot.sh`. The script will generate a svg file with the statistics of
each commit. The raw numbers that are plotted are currently hardcoded.

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
`available.ExecInfo` in available.py, which will allow for the other
scripts to process the new executable. For more details, see
available.ExecInfo.
