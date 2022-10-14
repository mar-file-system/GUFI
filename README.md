# Grand Unified File-Index (GUFI)

[![Core Tests](https://github.com/mar-file-system/GUFI/actions/workflows/test.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/test.yml)
[![Auxiliary Checks](https://github.com/mar-file-system/GUFI/actions/workflows/check.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/check.yml)
[![Documentation](https://github.com/mar-file-system/GUFI/actions/workflows/docs.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/docs.yml)
[![RPMs](https://github.com/mar-file-system/GUFI/actions/workflows/rpm.yml/badge.svg)](https://github.com/mar-file-system/GUFI/actions/workflows/rpm.yml)

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
git clone https://github.com/mar-file-system/GUFI.git
cd GUFI
mkdir build
cd build
cmake ..
make
sudo make install

# create a GUFI Index
gufi_dir2index <src_dir> <index_dir>
-or-
gufi_dir2trace <src_dir> <trace_file_prefix>
cat <trace_file_prefix>.* > <trace_file>
gufi_trace2index <trace_file> <index_dir>

# use the index
gufi_query/gufi_stat/bfti/bffuse/bfresultfuse/querydbs

# create /etc/GUFI/config from /etc/GUFI/config.example

# use gufi_query wrapper scripts
gufi_find/gufi_ls/gufi_stats/gufi_getfattr
```

For the dependency list, detailed build and install instructions, and common issues, please see [INSTALL](INSTALL).

## [Documentation](docs/README.md)

## Contact
gufi-lanl@lanl.gov
