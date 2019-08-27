#/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

. ${SCRIPT_PATH}/start_docker.sh

# remove unnecessary repositories
de bash -c "zypper repos | grep Yes | cut -f2 -d '|' | xargs zypper removerepo"
de zypper --non-interactive remove libapparmor* libgmodule* libX11* libxcb* mozilla* ncurses* qemu* vim*

# add the tumbleweed oss repo
de zypper ar -f -c http://download.opensuse.org/tumbleweed/repo/oss tumbleweed-oss
de zypper --non-interactive --no-gpg-checks update

# install libraries
de zypper --non-interactive install fuse-devel libattr-devel libmysqlclient-devel libuuid-devel pcre-devel

# install extra packages
de zypper --non-interactive install autoconf binutils cmake git libgcc_s1 patch pkg-config

if [[ "${C_COMPILER}" = gcc-* ]]; then
    C_PACKAGE="gcc${C_COMPILER##*-}"
    SUSE_C_COMPILER="gcc-${C_COMPILER##*-}"
elif [[ "${C_COMPILER}" = clang-* ]]; then
    C_PACKAGE="clang${C_COMPILER##*-}"
    SUSE_C_COMPILER="clang-${C_COMPILER##*-}.0"
else
    echo "Unknown C compiler: ${C_COMPILER}"
    exit 1
fi

if [[ "${CXX_COMPILER}" = g++-* ]]; then
    CXX_PACKAGE="gcc${CXX_COMPILER##*-}-c++"
    SUSE_CXX_COMPILER="g++-${CXX_COMPILER##*-}"
elif [[ "${CXX_COMPILER}" = clang++-* ]]; then
    CXX_PACKAGE="clang${CXX_COMPILER##*-}"
    SUSE_CXX_COMPILER="clang++-${CXX_COMPILER##*-}.0"
else
    echo "Unknown C++ compiler: ${CXX_COMPILER}"
    exit 1
fi

# install the compilers
# gcc must be installed even if compiling with clang
de zypper --non-interactive install gcc ${C_PACKAGE} ${CXX_PACKAGE}

# add the travis user
de useradd travis -m -s /sbin/nologin || true
de chown -R travis /GUFI

# build and test GUFI
docker exec --env C_COMPILER="${SUSE_C_COMPILER}" --env CXX_COMPILER="${SUSE_CXX_COMPILER}" --env BUILD="${BUILD}" --env CMAKE_FLAGS="${CMAKE_FLAGS}" --user travis "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && ${SCRIPT_PATH}/build_and_test.sh"
