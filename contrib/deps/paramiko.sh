#!/usr/bin/env bash
# build and install paramiko

set -e

# Assume all paths exist

paramiko_name="paramiko"
paramiko_prefix="${INSTALL}/${paramiko_name}"
if [[ ! -d "${paramiko_prefix}" ]]; then
    paramiko_build="${BUILD}/paramiko-cf7d49d66f3b1fbc8b0853518a54050182b3b5eb"
    if [[ ! -d "${paramiko_build}" ]]; then
        paramiko_tarball="${DOWNLOAD}/paramiko.tar.gz"
        if [[ ! -f "${paramiko_tarball}" ]]; then
            wget https://github.com/paramiko/paramiko/archive/cf7d49d66f3b1fbc8b0853518a54050182b3b5eb.tar.gz -O "${paramiko_tarball}"
        fi
    fi

    if [[ ! -d "${paramiko_build}" ]]; then
        tar -xf "${paramiko_tarball}" -C "${BUILD}"
        patch -d "${paramiko_build}" -p1 < "${SCRIPT_PATH}/paramiko-cf7d49d-hostbased.patch"
    fi
fi
