#!/usr/bin/env bash

set -e

ROOT="$(realpath $(dirname ${BASH_SOURCE[0]})/..)"
export PATH=${ROOT}/scripts:${PATH}
export PYTHONPATH=${ROOT}/scripts:${PYTHONPATH}

GUFI_STATS="${ROOT}/test/gufi_stats.py"

function get_next() {
    awk -F: "{id[\$3]=1}END{i = $1; while (i < 1000000) {if(id[i] == \"\"){print i; exit;} i++}}" "$2"
}

# find 2 unused UIDs
# UID1 should be the caller, since a non-existant UID would not be able to access this directory
UID1=$(id -u)
UID2=$(get_next $(( ${UID1} > 1004?(${UID1} + 1):1004 )) /etc/passwd)
UID3=$(get_next $((${UID2} + 1))                         /etc/passwd)

# find 2 unused IDs
GID1=$(id -g)
GID2=$(get_next $(( ${GID1} > 1004?(${GID1} + 1):1004 )) /etc/group)
GID3=$(get_next $((${GID2}  + 1))                        /etc/group)

CONFIG="config.test"
OUTPUT="gufi_stats.out"

function cleanup {
    sudo rm -rf "${CONFIG}" "${OUTPUT}" prefix prefix.gufi
}

trap cleanup EXIT

# generate a fake config file
echo "Threads=$(nproc)" > ${CONFIG}
echo "Exec=${ROOT}/src/gufi_query" >> ${CONFIG}
echo "IndexRoot=$(pwd)/prefix.gufi" >> ${CONFIG}
sudo chmod 666 ${CONFIG}

# generate the tree
sudo rm -rf prefix prefix.gufi
mkdir prefix
mkdir -p prefix/${UID1}
mkdir -p prefix/${UID2}
mkdir -p prefix/${UID3}
mkdir prefix/${UID2}/dir
truncate -s 1 prefix/${UID1}/f1
truncate -s 2 prefix/${UID2}/f1
truncate -s 2 prefix/${UID2}/f2
truncate -s 2 prefix/${UID2}/dir/f1
truncate -s 2 prefix/${UID2}/dir/f2
truncate -s 3 prefix/${UID3}/f1
truncate -s 3 prefix/${UID3}/f2
truncate -s 3 prefix/${UID3}/f3
ln -s prefix/${UID1}/f1 prefix/${UID1}/l1
ln -s prefix/${UID2}/f2 prefix/${UID2}/l1
ln -s prefix/${UID2}/f2 prefix/${UID2}/l2
ln -s prefix/${UID2}/f2 prefix/${UID2}/dir/l1
ln -s prefix/${UID2}/f2 prefix/${UID2}/dir/l2
ln -s prefix/${UID3}/f1 prefix/${UID3}/l1
ln -s prefix/${UID3}/f2 prefix/${UID3}/l2
ln -s prefix/${UID3}/f3 prefix/${UID3}/l3
sudo chown -R ${UID1}:${GID1} prefix/${UID1}
sudo chown -R ${UID2}:${GID2} prefix/${UID2}
sudo chown -R ${UID3}:${GID3} prefix/${UID3}
sudo chmod 700 prefix/${UID1}
sudo chmod 700 prefix/${UID2}
sudo chmod 700 prefix/${UID3}
sudo chmod 755 prefix
sudo chown 0:0 prefix

sudo ${ROOT}/src/gufi_dir2index -x prefix prefix.gufi

# replace text to get UIDs/GIDs 1001, 1002, and 1003
function replace() {
    echo "$@" | sed "s/${GUFI_STATS//\//\\/}/gufi_stats/g; s/\<${UID1}\>/1001/g; s/${UID2}\>/1002/g; s/\<${UID3}\>/1003/g; s/\<$(id -un)\>/1001/g; s/\<$(id -gn)\>/1001/g"
}

function root() {
    replace "# $@"
    output=$(sudo -E env "PATH=$PATH" env "PYTHONPATH=$PYTHONPATH" bash -c "$@")
    replace "${output}"
    echo
}

function user() {
    replace "$ $@"
    output=$(sudo -u \#${UID1} -E env "PATH=$PATH" env "PYTHONPATH=$PYTHONPATH" bash -c "$@")
    output="$($@)"
    replace "${output}"
    echo
}

(
root "${GUFI_STATS} depth"
root "${GUFI_STATS} depth -r"
root "${GUFI_STATS} depth ${UID2}"
root "${GUFI_STATS} depth ${UID2}/dir"
root "${GUFI_STATS} -r depth ${UID2}"
user "${GUFI_STATS} -r depth"

root "${GUFI_STATS} filesize"
root "${GUFI_STATS} -r filesize"
root "${GUFI_STATS} filesize ${UID2}"
root "${GUFI_STATS} -r filesize ${UID2}"
user "${GUFI_STATS} -r filesize"

root "${GUFI_STATS} filecount"
root "${GUFI_STATS} -r filecount"
root "${GUFI_STATS} filecount ${UID2}"
root "${GUFI_STATS} -r filecount ${UID2}"
user "${GUFI_STATS} -r filecount"

root "${GUFI_STATS} linkcount"
root "${GUFI_STATS} -r linkcount"
root "${GUFI_STATS} linkcount ${UID2}"
root "${GUFI_STATS} -r linkcount ${UID2}"
user "${GUFI_STATS} -r linkcount"

root "${GUFI_STATS} total-filesize"
root "${GUFI_STATS} -c total-filesize"
root "${GUFI_STATS} total-filesize ${UID2}"
root "${GUFI_STATS} -c total-filesize ${UID2}"
user "${GUFI_STATS} -c total-filesize"

root "${GUFI_STATS} total-filecount"
root "${GUFI_STATS} -c total-filecount"
root "${GUFI_STATS} total-filecount ${UID2}"
root "${GUFI_STATS} -c total-filecount ${UID2}"
user "${GUFI_STATS} -c total-filecount"

root "${GUFI_STATS} total-linkcount"
root "${GUFI_STATS} -c total-linkcount"
root "${GUFI_STATS} total-linkcount ${UID2}"
root "${GUFI_STATS} -c total-linkcount ${UID2}"
user "${GUFI_STATS} -c total-linkcount"

root "${GUFI_STATS} dircount"
root "${GUFI_STATS} -r dircount"
root "${GUFI_STATS} dircount ${UID2}"
root "${GUFI_STATS} -r dircount ${UID2}"
echo "###########################################"
echo "# This is wrong. This should be 3 but     #"
echo "# without root, the other databases can't #"
echo "# be accessed.                            #"
echo "###########################################"
user "${GUFI_STATS} -r dircount"

root "${GUFI_STATS} total-dircount"
root "${GUFI_STATS} -c total-dircount"
root "${GUFI_STATS} total-dircount ${UID2}"
root "${GUFI_STATS} -c total-dircount ${UID2}"
user "${GUFI_STATS} -c total-dircount"

root "${GUFI_STATS} leaf-dirs"
root "${GUFI_STATS} -r leaf-dirs"
root "${GUFI_STATS} leaf-dirs ${UID2}"
root "${GUFI_STATS} -r leaf-dirs ${UID2}"
user "${GUFI_STATS} -r leaf-dirs"

root "${GUFI_STATS} leaf-depth"
root "${GUFI_STATS} -r leaf-depth"
root "${GUFI_STATS} leaf-depth ${UID2}"
root "${GUFI_STATS} -r leaf-depth ${UID2}"
user "${GUFI_STATS} -r leaf-depth"

root "${GUFI_STATS} leaf-files"
root "${GUFI_STATS} -r leaf-files"
root "${GUFI_STATS} leaf-files ${UID2}"
root "${GUFI_STATS} -r leaf-files ${UID2}"
user "${GUFI_STATS} -r leaf-files"

root "${GUFI_STATS} total-leaf-files"
root "${GUFI_STATS} -c total-leaf-files"
root "${GUFI_STATS} total-leaf-files ${UID2}"
root "${GUFI_STATS} -c total-leaf-files ${UID2}"
user "${GUFI_STATS} -c total-leaf-files"

root "${GUFI_STATS} median-leaf-files"
root "${GUFI_STATS} median-leaf-files ${UID2}"
user "${GUFI_STATS} median-leaf-files"

root "${GUFI_STATS} files-per-level"
root "${GUFI_STATS} -c files-per-level"
root "${GUFI_STATS} files-per-level ${UID2}"
root "${GUFI_STATS} -c files-per-level ${UID2}"
user "${GUFI_STATS} -c files-per-level"

root "${GUFI_STATS} links-per-level"
root "${GUFI_STATS} -c links-per-level"
root "${GUFI_STATS} links-per-level ${UID2}"
root "${GUFI_STATS} -c links-per-level ${UID2}"
user "${GUFI_STATS} -c links-per-level"

root "${GUFI_STATS} dirs-per-level"
root "${GUFI_STATS} -c dirs-per-level"
root "${GUFI_STATS} dirs-per-level ${UID2}"
root "${GUFI_STATS} -c dirs-per-level ${UID2}"
user "${GUFI_STATS} -c dirs-per-level"
) | tee "${OUTPUT}"

diff -b ${ROOT}/test/gufi_stats.expected "${OUTPUT}"
