# GUFI Documentation
This directory contains documentation on what GUFI is, how this repository is structured,
how to build GUFI, and how to run GUFI.

## Design Documentation
Documentation on the design of GUFI can be found at [GUFI.docx](GUFI.docx) and in the Supercomputing 2022 paper:

Dominic Manno, Jason Lee, Prajwal Challa, Qing Zheng, David Bonnie, Gary Grider, and Bradley Settlemyer. 2022. GUFI: fast, secure file system metadata search for both privileged and unprivileged users. In Proceedings of the International Conference on High Performance Computing, Networking, Storage and Analysis (SC '22). IEEE Press, Article 57, 1–14. https://dl.acm.org/doi/abs/10.5555/3571885.3571960

## Usage
See the root [README](/README.md#quick-start) for quick start instructions.

For more detailed instructions, see the [user, administrator, and developer guides](latex) 
that will be placed into `docs/latex/pdfs` if building them is enabled with the CMake
configuration `-DLATEX_BUILD=On`.

[GUFI-SQL.docx](GUFI-SQL.docx) provides guidance on GUFI's SQL schema, functions, and usage.

## Directory Structure
- [config](/config)     - Example GUFI wrapper script configuration files
- [contrib](/contrib)   - Contains support files (cmake package finders, CI scripts, etc.)
- [docs](/docs)         - The documentation (you are here)
- [examples](/examples) - Scripts showing how to run GUFI to perform various tasks
- [include](/include)   - Headers
- [scripts](/scripts)   - Scripts that will be installed
- [src](/src)           - Source code for GUFI and executables
- [test](/test)         - GUFI tests

## [Executables](/src)
- [bffuse](bffuse)
- [bfresultfuse](bfresultfuse)
- [bfwreaddirplus2db](bfwreaddriplus2b)
- [dfw](dfw)
- [gufi_dir2index](gufi_dir2index)
- [gufi_dir2trace](gufi_dir2trace)
- [gufi_query](gufi_query)
- [gufi_trace2index](gufi_trace2index)
- [gufi_treesummary](gufi_treesummary)
- [make_testindex](make_testindex)
- [querydbs](querydbs)
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

## [Deploying GUFI](/examples/deploy)
- [Simple GUFI Server/Client](/examples/deploy/simple)

## Extra Information
- [Configuration Files](config)
- [Tests](tests)
