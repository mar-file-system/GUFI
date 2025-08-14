# Regression Tests

This directory contains regression tests for GUFI.

## Usage

Each regression test consists of at least two files: a bash script and
a corresponding .expected file (the regression tests are known answer
tests). There might be additional files, such as Python scripts,
associated with each test. All test files will be copied to
`test/regression` in the build directory. The scripts will
additionally be modified to replace CMake variables. The test files
will not be installed.

Always run the tests scripts located in the build directory, not the
ones located in the source directory. To run all tests (including
non-regression tests), run `ctest` or `make test`. To run a single
test, run the test script directly. All test scripts should pass when
running from any writable directory that allows for extended
attributes to be applied to filesystem entries. Note that some tests
require `sudo`. All other tests should pass with or without `sudo`.

The output of a test script will be printed to standard out as well as
a corresponding .out file. The diff between the .expected file and
.outfile will also be printed. If a regression test's .expected file
needs to be updated, the .out file can be used to directly overwrite
the .expected file with `cat` and a redirect, `cp`, or `mv`. The
.expected file should never need to be modified manually.

## Useful Utilities

- [`setup.sh`](setup.sh.in) contains common functions, variables, setup code, cleanup code, etc.
    - `replace`: replaces environment-specific text with common text,
      such as the actual path to an executable with the name
        - When adding a new test, add variables containing patterns to
          replace and modify this function to do the replacement

    - `run_sort`: runs the given command, printing the command as well
      as the sorted output, passed into `replace`

    - `run_no_sort`: runs the given command, printing the command as
      well as the unsorted output, passed into `replace`

    - `setup_cleanup`: function that is called on exit of script
        - Wrap this with a test-specific cleanup function if the test
          requires custom cleanup

- [`generatetree.sh`](generatetree.sh.in) generates a fixed source tree with known properties
    - This is the most commonly used script/tree
    - This is generally automatically called through sourcing `setup.sh`

- [`rollup_tree.sh`](rollup_tree.sh.in) generates a fixed tree with known properties for
  testing rollups with
    - Requires `sudo`

- [`gufi_tool.py`](gufi_tool.py.in) fakes entry points into Python tools in order to use
  a test configuration file instead of the real one

## Test Script Structure
1. `#!/usr/bin/env bash`
2. License
3. `set -e`
4. `. @CMAKE_CURRENT_BINARY_DIR@/setup.sh 1`
    - Pass in `0` if the source tree and index do not need to be generated
5. Define where the output will be written to
    - `OUTPUT="<name>.out"`
    - Should be to a file in the directory where the test script is being run from to make it easy to locate
6. Define other variables
7. If additional setup that creates artifacts is required
    - Create a `cleanup` function for this test's artifacts
        - On success, test scripts should not leave any artifacts
        - On failure, the only artifact left should be `"${OUTPUT}"`
    - Create a `cleanup_exit` function that runs both this test's `cleanup` as well as `setup_cleanup`
    - Set `cleanup_exit` to run on exit
        - `trap cleanup_exit EXIT`
    - Call `cleanup` once immediately to clean up the working directory
    - Do additional setup
        - The cleanup function has already been run and won't undo this setup
8. Run tests in a subshell
    - Use `run_sort`/`run_no_sort` to print commands and their outputs
    - `tee` all output to `"${OUTPUT}"`
9. `@DIFF@ @CMAKE_CURRENT_BINARY_DIR@/<name>.expected "${OUTPUT}"`
10. `rm "${OUTPUT}"`
    - This only happens if `@DIFF@` returned 0

## Adding a New Test Script
1. Create an empty test script with the name `<name>.sh.in` in this directory
2. Create an empty .expected file in this directory
3. Add the test script and .expected file to git
4. Add `<name>` to the appropriate location in [`CMakeLists.txt`](CMakeLists.txt)
5. Modify contents of the test script
    - See [Test Script Structure](#test-script-structure)
6. Rebuild to get the latest test script into the build directory
    - Will be called `<name>.sh`
7. Run the test script
8. If not satisfied with the tests or output, go to step 5
9. Overwrite the source .expected file with `<name>.out`
10. If available, run `make shellcheck` to find issues in the test script
11. Commit and push