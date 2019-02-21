#/usr/bin/env bash

set -e

# start at repository root
SCRIPT_PATH="$(dirname ${BASH_SOURCE[0]})"
cd ${SCRIPT_PATH}/../..

function ppde {
    docker exec "${TRAVIS_JOB_NUMBER}" "$@"
}

# . ${SCRIPT_PATH}/start_docker.sh

# # instsall Extra Packages for Enterprise Linux (EPEL)
# ppde yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

# # install The Software Collections ( SCL ) Repository
# ppde yum -y install centos-release-scl

# # install libraries
# ppde yum -y install fuse-devel libattr-devel libuuid-devel mariadb-devel pcre-devel

# # install extra packages
# ppde yum -y install cmake make redhat-lsb-core rh-git29 tcl wget
# # ppde update-alternatives --install /usr/bin/git git /opt/rh/rh-git29/root/usr/libexec/git-core/git 10
# ppde ln -sf /opt/rh/rh-git29/root/usr/libexec/git-core/git /usr/bin/git

if [[ "${C_COMPILER}" = gcc-* ]]; then
    VERSIONN="${C_COMPILER##*-}"
    C_PACKAGE="devtoolset-${VERSION}"
    CENTOS_C_COMPILER="gcc-${VERSION}"
    ppde update-alternatives --install /usr/bin/gcc-${VERSION} gcc-${VERSION} /opt/rh/devtoolset-${VERSION}/root/usr/bin/gcc 10
elif [[ "${C_COMPILER}" = clang ]]; then
    # llvm-toolset-7 installs clang-5.0
    C_PACKAGE="llvm-toolset-7"
    CENTOS_C_COMPILER="clang-5.0"
    ppde update-alternatives --install /usr/bin/clang-5.0 clang-5.0 /opt/rh/llvm-toolset-7/root/usr/bin/clang-5.0 10
else
    echo "Unknown C compiler: ${C_COMPILER}"
    exit 1
fi

if [[ "${CXX_COMPILER}" = g++-* ]]; then
    VERSION="${CXX_COMPILER##*-}"
    CXX_PACKAGE="devtoolset-${VERSION}-gcc-c++"
    CENTOS_CXX_COMPILER="g++-${VERSION}"
    ppde update-alternatives --install /usr/bin/g++-${VERSION} g++-${VERSION} /opt/rh/devtoolset-${VERSION}/root/usr/bin/g++ 10
elif [[ "${CXX_COMPILER}" = clang++ ]]; then
    # llvm-toolset-7 installs clang-5.0
    CXX_PACKAGE="llvm-toolset-7"
    CENTOS_CXX_COMPILER="clang++"
    ppde update-alternatives --install /usr/bin/clang++ clang++ /opt/rh/llvm-toolset-7/root/usr/bin/clang++ 10
else
    echo "Unknown C++ compiler: ${CXX_COMPILER}"
    exit 1
fi

# # install the compilers
# ppde yum -y install ${C_PACKAGE} ${CXX_PACKAGE}

# # install SQLite
# ppde wget -nc https://www.sqlite.org/2019/sqlite-autoconf-3270100.tar.gz
# ppde tar -xzf sqlite-autoconf-3270100.tar.gz
# ppde bash -c "cd sqlite-autoconf-3270100 && mkdir build && cd build && CC=${CENTOS_C_COMPILER} ../configure --prefix=/tmp/sqlite3 && make -j && make sqlite3.c && make -j install"

# # add the travis user
# ppde useradd travis -m -s /sbin/nologin || true
ppde chown -R travis /GUFI

# build and test GUFI
docker exec --env C_COMPILER="${CENTOS_C_COMPILER}" --env CXX_COMPILER="${CENTOS_CXX_COMPILER}" --env BUILD="${BUILD}" --user travis "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && mkdir -p build/googletest-download && cd build/googletest-download && cp ../../contrib/cmake/CMakeLists.txt.in CMakeLists.txt && cmake -G \"Unix Makefiles\" . && pwd && ls && cmake --build . && mv googletest-src .. && mv googletest-build .."

# docker exec --env C_COMPILER="${CENTOS_C_COMPILER}" --env CXX_COMPILER="${CENTOS_CXX_COMPILER}" --env BUILD="${BUILD}" --user travis "${TRAVIS_JOB_NUMBER}" bash -c "cd /GUFI && PKG_CONFIG_PATH=\"/tmp/sqlite3/lib/pkgconfig:\$(printenv PKG_CONFIG_PATH)\" ${SCRIPT_PATH}/build_and_test.sh"
