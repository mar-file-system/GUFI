# Grand Unified File-Index (GUFI)

GUFI is part of MarFS, which is released under the [BSD License](LICENSE.txt).
LA-CC-15-039

[![Build Status](https://travis-ci.com/mar-file-system/GUFI.svg?branch=master)](https://travis-ci.com/mar-file-system/GUFI)

## Documentation
Please see [README](README) and the [docs directory](docs) for documentation

## Dependencies
| Build Tools                     |  Comments  |
| :------------------------------ | :--------: |
| C compiler with C99 support     |            |
| C++ compiler with C++11 support | optional   |
| CMake 3.0+                      |            |

| Libraries     | Comments            |
| :------------ | :-----------------: |
| uuid          |                     |
| xattr.h       | Just the header     |
| sqlite3       | 3.13+               |
| pcre          | Not pcre2           |
| C-Thread-Pool | Downloaded by CMake |
| sqlite3-pcre  | Downloaded by CMake |
| Google Test   | Downloaded by CMake |
| FUSE          | optional            |
| MySQL         | optional            |
| db2           | optional            |

## Build
```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=<PATH> ..
make
make install
```
