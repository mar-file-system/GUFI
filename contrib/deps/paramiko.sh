#!/usr/bin/env bash
# build and install paramiko

set -e

# Assume all paths exist

paramiko_name="paramiko"
paramiko_prefix="${INSTALL_DIR}/${paramiko_name}"
if [[ ! -d "${paramiko_prefix}" ]]; then
    paramiko_build="${BUILD_DIR}/paramiko-master"
    if [[ ! -d "${paramiko_build}" ]]; then
        paramiko_tarball="${DOWNLOAD_DIR}/paramiko.tar.gz"
        if [[ ! -f "${paramiko_tarball}" ]]; then
            wget https://github.com/paramiko/paramiko/archive/master.tar.gz -O "${paramiko_tarball}"
        fi
    fi

    if [[ ! -d "${paramiko_build}" ]]; then
        tar -xf "${paramiko_tarball}" -C "${BUILD_DIR}"
        patch -d "${paramiko_build}" -p1 < "${SCRIPT_PATH}/paramiko.patch"
    fi
fi
