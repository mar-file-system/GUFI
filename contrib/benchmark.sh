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
THREADS="2"
BFQ=src/bfq

if [[ "$#" -gt 2 ]]; then
    THREADS="$3"
fi

NUMACTL="numactl -m 5 -N 5"
COMMON="--threads ${THREADS} --bfq ${BFQ}"

echo
echo "Top 10 Users taking up the most space"
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} --order most --num_results 10 space ${GUFI_TREE}

echo
echo "Total Number of Files"
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} total-files ${GUFI_TREE}

echo
echo "Total Space Taken"
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} total-space ${GUFI_TREE}

echo
echo "Top 10 Users with the most files"
MOST_FILES="$(sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} --order most --num_results 10 files ${GUFI_TREE})"
echo "${MOST_FILES}"

MOST_FILES="$(echo "${MOST_FILES}" | head -n 1 | awk -F'|' '{printf $1}')"

echo
echo "Number of files owned by user ${MOST_FILES} (gufi_find)"
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_find ${COMMON} --no-aggregate ${GUFI_TREE} | wc -l

echo
echo "Number of files owned by user ${MOST_FILES} (gufi_stats)"
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_stats ${COMMON} total-files ${GUFI_TREE}

echo
echo "Number of files owned by user ${MOST_FILES} larger than 1KB (gufi_find)"
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_find ${COMMON} --no-aggregate -size=+1024c ${GUFI_TREE} | wc -l
