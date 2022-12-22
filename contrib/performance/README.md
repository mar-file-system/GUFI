# Performance Tracking and Graphing

The performance history framework tracks the performance of GUFI. It contains scripts to store debug values and generate graphs.

Prerequisites
-------------

* `git 1.8.5+`

* `Python 2.7+`

* GUFI must be built with **Debug** mode enabled and one of the debug prints supported by extract.py (currently only cumulative_times)

Setup:
------

**NOTE:** Running through the workflow below may not yield the same hashes. This workflow is meant to be a generic example.

**NOTE:** This is run inside of the GUFI/build/contrib/performance directory

1. Setup Python Enviornment:
    ```
    export PYTHONPATH="GUFI/build/contrib:${PYTHONPATH}"
    export PYTHONPATH="GUFI/build/contrib/performance:${PYTHONPATH}"
    export PYTHONPATH="GUFI/build/scripts:${PYTHONPATH}"
    ```

2. Use **setup_hashdb.py** to initialize a database to store hashes:

    * **Syntax:** `setup_hashdb.py <hashdb_name>`

    * **Example:** `setup_hashdb.py hashes.db`

    * **Output:** (`hashes.db` file created)

3. Use **gufi_hash.py** to hash GUFI command and store in hash database:

    * **Syntax:** `gufi_hash.py <gufi_command> <debug_values> <gufi_index> [gufi command flags][--database <hashdb_name>]`

        * **NOTE:** Without **--database** the hash is calculated, but not stored

        * **NOTE:** See `gufi_hash.py -h` for full list of command options

    * **Example:** `gufi_hash.py gufi_query cumulative_times_terse /path/to/tree -S "SELECT uid FROM summary;" -n 112 -j --database hashes.db`

    * **Output:** `0b5c2dbf57a397906f1d4bf7cf4fae4c` (will be stored in **hashes.db** in the **gufi_command** table)

4. Use **machine_hash.py** to hash machine attributes and store in hash database:

    * **Syntax:** `machine_hash.py [machine command flags] [--database <hashdb_name>]`

        * **NOTE:** Without **--database** the hash is calculated, but not stored

        * **NOTE:** This will run with **no arguments**.

        * **NOTE:** see `machine_hash.py -h` for full list of command flags

    * **Example:** `machine_hash.py --cores 8 --ram 8GB --database hashes.db`

    * **Output:** `d15e2506a6c1664f7e931b55f39d06fd` (will be stored in **hashes.db** in the **machine_config** table)

5. Use **raw_data_hash.py** to generate a combined hash using the machine_hash and gufi_hash as inputs:

    * **Syntax:** `raw_data_hash.py <machine_hash> <gufi_hash> [--database <hashdb_name>]`

        * **NOTE:** Without **--database** the hash is calculated, but not stored

    * **Example:** `raw_data_hash.py d15e2506a6c1664f7e931b55f39d06fd 0b5c2dbf57a397906f1d4bf7cf4fae4c --database hashes.db`

    * **Output:** `825577c396836cdaa6491b7bfb6901c9` (will be stored in **hashes.db** in the **raw_data_dbs** table)

6. Use **setup_raw_data_db.py** to initialize a raw data database:

    * **Syntax:** `setup_raw_data_db.py <hashdb_name> <raw_hash> <raw_data_db_name>`

    * **Example:** `setup_raw_data_db.py hashes.db 825577c396836cdaa6491b7bfb6901c9 raw_numbers.db`

    * **Output:** `825577c396836cdaa6491b7bfb6901c9 was run with gufi_query, debug name cumulative_times` (`raw_numbers.db` file created)

Database Tables
---------------

1. hashdb (Result of running: **setup_hashdb.py**)

    * gufi_command
    * machine_config
    * raw_data_dbs (raw_hash|machine_hash|gufi_hash)

2. raw_data_db (Result of running: **setup_raw_data_db.py**, based on debug values stored)

    * cumulative_times

Inserting debug data
---------------------

Use **extraction.py** to extract a single set of debug values and store in database

* **Syntax:** `<gufi_cmd> 2>&1 >/dev/null | <extraction.py_path> <hashdb_name> <raw_hash> <raw_data_db_name>`

* **Example:** `GUFI/build/src/gufi_query -S "SELECT uid FROM summary" ./tree 2>&1 >/dev/null | extraction.py hashes.db 825577c396836cdaa6491b7bfb6901c9 raw_numbers.db`

* **Output:** `825577c396836cdaa6491b7bfb6901c9 was run with gufi_query, debug name cumulative_times` (resulting cumulative_times be stored in `raw_numbers.db`)

Collect and insert debug data across commits
--------------------------------------------

While in most cases, a user can gather cumulative times data run by run, in some cases, a user will want to use the `collect_performance.sh` script to automatically collect data across commits. Should this be the case, the user will need to download a separate GUFI repo for acquiring performance numbers that will switch between commits (`GUFI_variable`).

This task has been greatly simplified and can be accomplished using `collect_performance.sh` inside of the first GUFI build.

* **Syntax:** `collect_performance.sh <gufi_variable_build> <hashdb_name> <raw_hash> <raw_data_db_name> <commits> [--extract <path_to_extract>] [--runs <runs_on_each_commit>] [--build-threads <thread count>] [--sudo]`

    * **Runs:** How many times to run the debug command on a given commit

    * **Frequency:** Rate at which to change commits in a given range (Defined with @)

    * **\<commits\>** can be any of:

        * **Single commit:** commit1
        * **Commit range:** commit2..commit3
            - with frequencies: commit4..commit56@7

* **Example:** `collect_performance.sh GUFI_variable/build/ hashes.db 825577c396836cdaa6491b7bfb6901c9 raw_numbers.db master~300..master@15 --build-threads 64 --runs 2 --sudo`

* **Output:** 
``` 
HEAD is now at c8cdd6e Collect performance will fail if build fails or extraction fails
825577c396836cdaa6491b7bfb6901c9 was run with gufi_query, debug name cumulative_times_terse 
825577c396836cdaa6491b7bfb6901c9 was run with gufi_query, debug name cumulative_times_terse
...
Previous HEAD position was 03e525e test server/client rpms
HEAD is now at 5e928ac cleanup
825577c396836cdaa6491b7bfb6901c9 was run with gufi_query, debug name cumulative_times_terse
825577c396836cdaa6491b7bfb6901c9 was run with gufi_query, debug name cumulative_times_terse
```

***************************************************************************

Graphing
--------

**Prerequisite:** Must have the latest **Matplotlib** Python package for user's Python version

Use **graph_performance.py** to plot data with user configurations

* **Syntax:** `graph_performance.py <hashdb_name> <rawhash> <raw_data_db_name> <config_file> [overwrite config arguments]`

* **Example:** `graph_performance.py hashes.db 825577c396836cdaa6491b7bfb6901c9 raw_numbers.db configs/db_query.ini`

* **Output:** 

<img title="Example Graph" src="configs/example.png" width="800" height="400">