#!/usr/bin/env bash

set -e

export PATH="@CMAKE_CURRENT_BINARY_DIR@:@DEP_INSTALL_PREFIX@/sqlite3/bin:${PATH}"
export LD_LIBRARY_PATH="@DEP_INSTALL_PREFIX@/sqlite3/lib:${LD_LIBRARY_PATH}"
export PYTHONPATH="@CMAKE_BINARY_DIR@/contrib:@CMAKE_BINARY_DIR@/contrib/performance:@CMAKE_BINARY_DIR@/scripts:${PYTHONPATH}"

# database containing hashes of machine properties and gufi commands
HASHDB="hashes.db"

# database containing raw data (gufi_query + cumulative times)
RAW_DATA_DB="example_raw_data.db"

# config file describing what to graph
CONFIG_FILE="@CMAKE_CURRENT_BINARY_DIR@/configs/db_test.ini"

GUFI_CMD="gufi_query"
DEBUG_NAME="cumulative-times"
TREE="test_tree"
OVERRIDE="custom_hash_value"

cleanup() {
    rm -f "${HASHDB}" "${RAW_DATA_DB}"

    # delete generated graph
    rm -f "$(grep path ${CONFIG_FILE} | awk '{ print $3 }')"
}

cleanup

# setup hash database and insert hashes
setup_hashdb.py "${HASHDB}"
machine_hash=$(machine_hash.py --database "${HASHDB}")
gufi_hash=$(gufi_hash.py --database "${HASHDB}" "${GUFI_CMD}" "${DEBUG_NAME}" "${TREE}")
raw_data_hash=$(raw_data_hash.py --database "${HASHDB}" --override "${OVERRIDE}" "${machine_hash}" "${gufi_hash}")

# setup raw data database
setup_raw_data_db.py "${HASHDB}" "${raw_data_hash}" "${RAW_DATA_DB}"

for((commit=0; commit<5; commit++))
do

for((x=0; x<5; x++))
do
(
# fake numbers
cat <<EOF
set up globals:                             1.00s
set up intermediate databases:              1.00s
thread pool:                                10.00s
    open directories:                       1.00s
    attach index:                           1.00s
    xattrprep:                              0.00s
    addqueryfuncs:                          0.00s
    get_rollupscore:                        0.00s
    descend:                                0.00s
        check args:                         0.00s
        check level:                        0.00s
        check level <= max_level branch:    0.00s
        while true:                         0.00s
            readdir:                        0.00s
            readdir != null branch:         0.00s
            strncmp:                        0.00s
            strncmp != . or ..:             0.00s
            snprintf:                       0.00s
            lstat:                          0.00s
            isdir:                          0.00s
            isdir branch:                   0.00s
            access:                         0.00s
            set:                            0.00s
            clone:                          0.00s
            pushdir:                        0.00s
    check if treesummary table exists:      0.00s
    sqltsum:                                0.00s
    sqlsum:                                 1.00s
    sqlent:                                 5.00s
    xattrdone:                              0.00s
    detach index:                           1.00s
    close directories:                      1.00s
    restore timestamps:                     0.00s
    free work:                              0.00s
    output timestamps:                      0.00s
aggregate into final databases:             1.00s
print aggregated results:                   1.00s
clean up globals:                           1.00s

Threads run:                                1024
Queries performed:                          2048
Rows printed to stdout or outfiles:         4096
Total Thread Time (not including main):     $((10 + x)).00s
Real time (main):                           ${x}.00s
EOF
) | extract.py "${HASHDB}" --commit "commit-${commit}" --branch "example" "${raw_data_hash}" "${RAW_DATA_DB}"
done
done

graph_performance.py "${HASHDB}" "${raw_data_hash}" "${RAW_DATA_DB}" "${CONFIG_FILE}"
