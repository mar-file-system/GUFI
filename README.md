# Grand Unified File-Index (GUFI)

[![Build Status](https://travis-ci.com/mar-file-system/GUFI.svg?branch=master)](https://travis-ci.com/mar-file-system/GUFI)
[![Latest Release](https://img.shields.io/github/release/mar-file-system/GUFI.svg?style=popout)](https://github.com/mar-file-system/GUFI/releases/latest)
[![Downloads](https://img.shields.io/github/downloads/mar-file-system/GUFI/latest/total.svg?style=popout)](https://github.com/mar-file-system/GUFI/releases/latest)

GUFI is part of MarFS, which is released under the [BSD License](LICENSE.txt).
LA-CC-15-039

Initial development was done at LANL via an internal git-hosting service.
As of rev 0.1.0, we have moved GUFI to GitHub:

    https://github.com/mar-file-system/GUFI

We invite anyone to contribute suggestions, new features, bug-reports,
bug-fixes, feature-requests, etc, through GitHub:

    https://github.com/mar-file-system/GUFI/issues

All bug-reports, issues, requests, etc, should go through GitHub.
We will attempt to respond to requests, but we can't make promises about
the level of resources that will be dedicated to GUFI maintenance.

See other components of MarFS at:

    https://github.com/mar-file-system

## Quick Start
```bash
mkdir build
cd build
cmake ..
make
make install

# create a GUFI Index
gufi_dir2index <src_dir> <index_dir>
-or-
gufi_dir2trace -o <trace_file_prefix> <src_dir>
cat <trace_file_prefix>.* > <trace_file>
gufi_trace2index <trace_file> <index_dir>

# create /etc/GUFI/config from /etc/GUFI/config.example

# use the index
gufi_query/gufi_find/gufi_ls/gufi_stats/gufi_stat/bfti/bffuse/bfresultfuse/querydb/querydbn
```

For the dependency list, detailed build and install instructions, and common issues, please see [INSTALL](INSTALL).

## [Documentation](docs/README.md)

## Contact
gufi-lanl@lanl.gov
