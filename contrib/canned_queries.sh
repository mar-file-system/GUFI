#!/usr/bin/env bash

set -e

if [[ "$#" -lt 1 ]]; then
    echo "Syntax: $0 GUFI_scripts_prefix"
    exit 1
fi

# if [[ "$UID" -ne 0 ]]; then
#     echo "This script requires sudo"
#     exit 1
# fi

GUFI_PREFIX="$(realpath $1)"

NUMACTL= # "numactl -m 5 -N 5"

echo
echo "Top 10 Users taking up the most space"
echo 3 > /proc/sys/vm/drop_caches
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats --order most --num_results 10 space

echo
echo "Total Number of Files"
echo 3 > /proc/sys/vm/drop_caches
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats total-files

echo
echo "Total Space Taken"
echo 3 > /proc/sys/vm/drop_caches
sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats total-space

echo
echo "Top 10 Users with the most files"
echo 3 > /proc/sys/vm/drop_caches
MOST_FILES="$(sudo ${NUMACTL} ${GUFI_PREFIX}/gufi_stats --order most --num_results 10 files)"
echo "${MOST_FILES}"

MOST_FILES="$(echo "${MOST_FILES}" | head -n 1 | awk -F'|' '{printf $1}')"

echo
echo "Number of files owned by user ${MOST_FILES} (gufi_find)"
echo 3 > /proc/sys/vm/drop_caches
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_find | wc -l

echo
echo "Number of files owned by user ${MOST_FILES} (gufi_stats)"
echo 3 > /proc/sys/vm/drop_caches
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_stats total-files

echo
echo "Number of files owned by user ${MOST_FILES} larger than 1KB (gufi_find)"
echo 3 > /proc/sys/vm/drop_caches
sudo -u \#"${MOST_FILES}" ${NUMACTL} ${GUFI_PREFIX}/gufi_find -size=+1024c | wc -l
