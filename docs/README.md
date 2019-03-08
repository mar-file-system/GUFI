# GUFI Documentation
This directory contains documentation on what GUFI is, how this repository is structured,
how to build GUFI, and how to run GUFI.

## Directory Structure
- [contrib](/contrib)   - Contains support files (cmake package finders, Travis CI scripts, etc.)
- [docs](/docs)         - The documentation (you are here)
- [examples](/examples) - Scripts showing how to run GUFI to perform various tasks
- [include](/include)   - Headers
- [scripts](/scripts)   - Scripts that will be installed
- [src](/src)           - Source code for GUFI and executables
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
- [diffreadirplusdb](/examples/diffreadirplusdb)
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
- [runtsm](/examples/runtsm)
- [userfilespacehog](/examples/userfilespacehog)
- [userfilespacehogusesummary](/examples/userfilespacehogusesummary)

## Extra Information
- [SQL Schemas](SQLSchemas)
- [SQL Functions](SQLFunctions)
- [Tests](tests)
- [Notes](NOTES.txt)