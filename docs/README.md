# GUFI Documentation
This directory contains documentation on what GUFI is, how this repository is structured,
how to build GUFI, and how to run GUFI.

## Development

Initial development was done at LANL via an internal git-hosting service.
As of rev 0.1.0, we are moving GUFI to github:

    https://github.com/mar-file-system/GUFI

We invite anyone to contribute suggestions, new features, bug-reports,
bug-fixes, feature-requests, etc, through github:

    https://github.com/mar-file-system/GUFI/issues

All bug-reports, issues, requests, etc, should go through github.
We will attempt to respond to requests, but we can't make promises about
the level of resources that will be dedicated to GUFI maintenance.

For other conversations about GUFI, there is:

    gufi-lanl@lanl.gov

See other components of MarFS at:

    https://github.com/mar-file-system.

## Directory Structure
- [contrib](/contrib)   - Contains support files (cmake package finders, Travis CI scripts, etc.)
- [docs](/docs)         - The documentation (you are here)
- [examples](/examples) - Scripts showing how to run GUFI to perform various tasks
- [include](/include)   - Headers
- [scripts](/scripts)   - Scripts that will be installed
- [test](/test)         - GUFI tests

## Full Documentation
The full documentation can be found at [GUFI.docx](GUFI.docx).

## How To Build and Install
Please see [INSTALL](/INSTALL).

## [Executables](/src)
- [bffuse](bffuse)
- [bfmi](bfmi)
- [bfq](bfq)
- [bfresultfuse](bfresultfuse)
- [bfti](bfti)
- [bfwi](bfwi)
- [bfwreaddirplus2db](bfwreaddriplus2b)
- [dfw](dfw)
- [make_testdirs](make_testdirs)
- [make_testtree](make_testtree)
- [querydb](querydb)
- [querydbn](querydbn)
- [tsmepoch2time](tsmepoch2time)
- [tsmtime2epoch](tsmtime2epoch)

## [Running GUFI](/examples)
- [deluidgidsummaryrecs](/examples/deluidgidsummaryrecs)
- [example_run](/examples/example_run)
- [generategidsummary](/examples/generategidsummary)
- [generateuidsummary](/examples/generateuidsummary)
- [gengidsummaryavoidentriesscan](/examples/gengidsummaryavoidentriesscan)
- [genuidsummaryavoidentriesscan](/examples/genuidsummaryavoidentriesscan)
- [groupfilespacehog](/examples/groupfilespacehog)
- [groupfilespacehogusesummary](/examples/groupfilespacehogusesummary)
- [listschemadb](/examples/listschemadb)
- [listtablesdb](/examples/listtablesdb)
- [oldbigfiles](/examples/oldbigfiles)
- [userfilespacehog](/examples/userfilespacehog)
- [userfilespacehogusesummary](/examples/userfilespacehogusesummary)

## Extra Information
- [SQL Schemas](SQLSchemas)
- [SQL Functions](SQLFunctions)
- [Tests](tests)
- [Notes](NOTES.txt)