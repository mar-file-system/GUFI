# Grand Unified File-Index (GUFI)

GUFI is part of MarFS, which is released under the [BSD License](LICENSE.txt).

[![Build Status](https://travis-ci.com/mar-file-system/GUFI.svg?branch=master)](https://travis-ci.com/mar-file-system/GUFI)

## Documentation
Please see [README](README) and the [docs directory](docs) for documentation

## Dependencies

### Build Tools
- Modern C compiler
- Modern C++ compiler
- CMake 3

### Libraries
- sqlite3
- pcre
- C-Thread-Pool (downloaded by CMake)
- sqlite3-pcre (downloaded by CMake)
- Google Test (downloaded by CMake)
- FUSE (optional)
- MySQL (optional)
- db2 (optional)

## Build
```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=<PATH> ..
make
make install
```
Note: `-DCMAKE_INSTALL_PREFIX=<PATH>` is almost always required because
CMake will install `C-Thread-Pool` and `sqlite3-pcre` at build time, and
will try to install into `/usr/local` if a path is not provided.