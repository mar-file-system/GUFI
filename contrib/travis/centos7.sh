#/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

. ${SCRIPT_PATH}/start_docker.sh

# install Extra Packages for Enterprise Linux (EPEL) and The Software Collections (SCL) Repository
de yum -y install epel-release centos-release-scl

# install libraries
de yum -y install fuse-devel libattr1 pcre-devel

# install extra packages
de yum -y install cmake3 make patch pkgconfig rh-git29

# create symlinks
de ln -sf /opt/rh/rh-git29/root/usr/libexec/git-core/git /usr/bin/git
de ln -sf /usr/bin/cmake3 /usr/bin/cmake
de ln -sf /usr/bin/ctest3 /usr/bin/ctest

if [[ "${C_COMPILER}" = gcc-* ]]; then
    VERSIONN="${C_COMPILER##*-}"
    C_PACKAGE="devtoolset-${VERSION}"
    CENTOS_C_COMPILER="gcc-${VERSION}"
    de update-alternatives --install /usr/bin/gcc-${VERSION} gcc-${VERSION} /opt/rh/devtoolset-${VERSION}/root/usr/bin/gcc 10
elif [[ "${C_COMPILER}" = clang ]]; then
    # llvm-toolset-7 installs clang-5.0
    C_PACKAGE="llvm-toolset-7"
    CENTOS_C_COMPILER="clang-5.0"
    de update-alternatives --install /usr/bin/clang-5.0 clang-5.0 /opt/rh/llvm-toolset-7/root/usr/bin/clang-5.0 10
else
    echo "Unknown C compiler: ${C_COMPILER}"
    exit 1
fi

if [[ "${CXX_COMPILER}" = g++-* ]]; then
    VERSION="${CXX_COMPILER##*-}"
    CXX_PACKAGE="devtoolset-${VERSION}-gcc-c++"
    CENTOS_CXX_COMPILER="g++-${VERSION}"
    de update-alternatives --install /usr/bin/g++-${VERSION} g++-${VERSION} /opt/rh/devtoolset-${VERSION}/root/usr/bin/g++ 10
elif [[ "${CXX_COMPILER}" = clang++ ]]; then
    # llvm-toolset-7 installs clang-5.0
    CXX_PACKAGE="llvm-toolset-7"
    CENTOS_CXX_COMPILER="clang++"
    de update-alternatives --install /usr/bin/clang++ clang++ /opt/rh/llvm-toolset-7/root/usr/bin/clang++ 10
else
    echo "Unknown C++ compiler: ${CXX_COMPILER}"
    exit 1
fi

# install the compilers
de yum -y install ${C_PACKAGE} ${CXX_PACKAGE}

# add the travis user
de useradd travis -m -s /sbin/nologin || true
de chown -R travis /GUFI

# build and test GUFI
docker exec --env C_COMPILER="${CENTOS_C_COMPILER}" --env CXX_COMPILER="${CENTOS_CXX_COMPILER}" --env BUILD="${BUILD}" --user travis "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && LD_LIBRARY_PATH=\"/opt/rh/httpd24/root/usr/lib64/:\$(printenv LD_LIBRARY_PATH)\" PKG_CONFIG_PATH=\"/tmp/sqlite3/lib/pkgconfig:\$(printenv PKG_CONFIG_PATH)\" ${SCRIPT_PATH}/build_and_test.sh"
