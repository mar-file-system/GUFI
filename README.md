# Grand Unified File-Index (GUFI)

[![Core Tests](https://github.com/mar-file-system/GUFI/actions/workflows/test.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/test.yml)
[![Auxiliary Checks](https://github.com/mar-file-system/GUFI/actions/workflows/check.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/check.yml)
[![Documentation](https://github.com/mar-file-system/GUFI/actions/workflows/docs.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/docs.yml)
[![Install](https://github.com/mar-file-system/GUFI/actions/workflows/install.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/install.yml)
[![codecov](https://codecov.io/github/mar-file-system/GUFI/branch/main/graph/badge.svg?token=VIOCZC7KIO)](https://app.codecov.io/github/mar-file-system/GUFI/tree/main)

[![Latest Release](https://img.shields.io/github/release/mar-file-system/GUFI.svg?style=popout)](https://github.com/mar-file-system/GUFI/releases/latest)
[![Downloads](https://img.shields.io/github/downloads/mar-file-system/GUFI/total)](https://github.com/mar-file-system/GUFI/releases)

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
git clone https://github.com/mar-file-system/GUFI.git
cd GUFI
mkdir build
cd build
cmake ..
make
(sudo) make install

# create a GUFI tree
gufi_dir2index <src_dir> <index_dir>
-or-
gufi_dir2trace <src_dir> <trace_file_prefix>
cat <trace_file_prefix>.* > <trace_file>
gufi_trace2index <trace_file> <index_dir>

# post-process the index
gufi_treesummary/gufi_treesummary_all/gufi_rollup

# query the index
gufi_query

# create /etc/GUFI/config from /etc/GUFI/server.example

# use wrapper scripts
gufi_find/gufi_ls/gufi_getfattr/gufi_stat/gufi_stats
```

For the dependency list, detailed build and install instructions, and details about GUFI, please see the [documentation](docs/README.md).

## Contact
gufi-lanl@lanl.gov
