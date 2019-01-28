# Grand Unified File-Index (GUFI)

GUFI is part of MarFS, which is released under the [BSD License](LICENSE.txt).

## Documentation
Please see [README](README) and the [docs directory](docs) for documentation

## Dependencies

### Programs
- Modern C compiler
- Modern C++ compiler
- CMake 3

### Libraries
- sqlite3
- pcre
- FUSE (optional)
- MySQL (optional)
- db2 (optional)
- C-Thread-Pool (downloaded by CMake)
- sqlite3-pcre (downloaded by CMake)
- Google Test (downloaded by CMake)

## Build
```
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=<PATH> ..
make
make install
```