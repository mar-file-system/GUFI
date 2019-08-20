#!/usr/bin/env bash

set -e

if [[ "$#" -lt 2 ]]; then
    echo "Syntax: $0 GUFI_scripts_prefix GUFI_tree [threads]"
    exit 1
fi

# if [[ "$UID" -ne 0 ]]; then
#     echo "This script requires sudo"
#     exit 1
# fi

GUFI_PREFIX="$(realpath $1)"
GUFI_TREE="$(realpath $2)"
THREADS="16"
BFQ=src/gufi_query

if [[ "$#" -gt 2 ]]; then
    THREADS="$3"
fi

NUMACTL= # "numactl -m 5 -N 5"
COMMON="--threads ${THREADS} --exec ${BFQ}"

echo
echo "Top 10 Users taking up the most space"
echo 3 > /proc/sys/vm/drop_caches
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} --order most --num_results 10 space ${GUFI_TREE}

echo
echo "Total Number of Files"
echo 3 > /proc/sys/vm/drop_caches
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} total-files ${GUFI_TREE}

echo
echo "Total Space Taken"
echo 3 > /proc/sys/vm/drop_caches
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} total-space ${GUFI_TREE}

echo
echo "Top 10 Users with the most files"
echo 3 > /proc/sys/vm/drop_caches
MOST_FILES="$(sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} --order most --num_results 10 files ${GUFI_TREE})"
echo "${MOST_FILES}"

MOST_FILES="$(echo "${MOST_FILES}" | head -n 1 | awk -F'|' '{printf $1}')"

echo
echo "Number of files owned by user ${MOST_FILES} (gufi_find)"
echo 3 > /proc/sys/vm/drop_caches
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_find ${COMMON} ${GUFI_TREE} | wc -l

echo
echo "Number of files owned by user ${MOST_FILES} (gufi_stats)"
echo 3 > /proc/sys/vm/drop_caches
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} total-files ${GUFI_TREE}

echo
echo "Number of files owned by user ${MOST_FILES} larger than 1KB (gufi_find)"
echo 3 > /proc/sys/vm/drop_caches
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_find ${COMMON} -size=+1024c ${GUFI_TREE} | wc -l
