# Grand Unified File-Index (GUFI)

GUFI is part of MarFS, which is released under the [BSD License](LICENSE.txt).
LA-CC-15-039

[![Build Status](https://travis-ci.com/mar-file-system/GUFI.svg?branch=master)](https://travis-ci.com/mar-file-system/GUFI)

## Documentation
Please see [README](README) and the [docs directory](docs) for documentation

## Dependencies

### Build Tools
- C compiler with C99 support
- C++ compiler with C++11 support
- CMake 3.0+

### Libraries
- uuid
- sqlite3 (at least version 3.13)
- pcre (not pcre2)
- C-Thread-Pool (downloaded by CMake)
- sqlite3-pcre (downloaded by CMake)
- Google Test (downloaded by CMake)
- FUSE (optional)
- MySQL (optional)
- db2 (optional)

### Others
- lsb_release (on Linux)
- xattr support (optional)

## Build
```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=<PATH> ..
make
make install
```
