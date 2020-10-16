#!/usr/bin/env bash
# build and install gnuplot

set -e

# Assume all paths exist

gnuplot_name="gnuplot"
gnuplot_prefix="${INSTALL_DIR}/${gnuplot_name}"
if [[ ! -d "${gnuplot_prefix}" ]]; then
    gnuplot_build="${BUILD_DIR}/gnuplot-5.4.0"
    if [[ ! -d "${gnuplot_build}" ]]; then
        gnuplot_tarball="${DOWNLOAD_DIR}/gnuplot-5.4.0.tar.gz"
        if [[ ! -f "${gnuplot_tarball}" ]]; then
            wget https://downloads.sourceforge.net/project/gnuplot/gnuplot/5.4.0/gnuplot-5.4.0.tar.gz -O "${gnuplot_tarball}"
        fi

        tar -xf "${gnuplot_tarball}" -C "${BUILD_DIR}"
    fi

    cd "${gnuplot_build}"
    mkdir -p build
    cd build
    if [[ ! -f Makefile ]]; then
        ../configure --prefix="${gnuplot_prefix}"
    fi
    make
    make -i install
fi
