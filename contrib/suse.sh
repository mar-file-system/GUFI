#/usr/bin/env bash

set -e

function ppde {
    docker exec "${TRAVIS_JOB_NUMBER}" "$@"
}

docker container stop $(docker container ls -aq) || true
docker container rm   $(docker container ls -aq) || true

# get the image
docker pull "${DOCKER_IMAGE}"

# start container
docker run -it -d --name "${TRAVIS_JOB_NUMBER}" -v $(realpath .):/GUFI -w /GUFI "${DOCKER_IMAGE}" bash

# remove unnecessary repositories
ppde bash -c "zypper repos | grep Yes | cut -f2 -d '|' | xargs zypper removerepo"
ppde zypper --non-interactive remove libapparmor* libgmodule* libX11* libxcb* mozilla* ncurses* qemu* vim*

# add the tumbleweed oss repo
ppde zypper ar -f -c http://download.opensuse.org/tumbleweed/repo/oss tumbleweed-oss
ppde zypper --non-interactive --no-gpg-checks update

# install libraries
ppde zypper --non-interactive install fuse-devel libattr-devel libmysqlclient-devel libuuid-devel pcre-devel sqlite3-devel

# install extra packages
ppde zypper --non-interactive install binutils cmake git libgcc_s1 lsb-release sqlite3

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
ppde zypper --non-interactive install gcc ${C_PACKAGE} ${CXX_PACKAGE}

# add the travis user
ppde useradd travis -m -s /sbin/nologin || true
ppde chown -R travis /GUFI

# build and test GUFI
docker exec --env C_COMPILER="${SUSE_C_COMPILER}" --env CXX_COMPILER="${SUSE_CXX_COMPILER}" --env BUILD="${BUILD}" --user travis "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && contrib/build_and_test.sh"
