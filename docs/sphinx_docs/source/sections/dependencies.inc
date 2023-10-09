..
    This file is part of GUFI, which is part of MarFS, which is released
    under the BSD license.


    Copyright (c) 2017, Los Alamos National Security (LANS), LLC
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation and/or
    other materials provided with the distribution.

    3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software without
    specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
    IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
    INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
    OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
    ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


    From Los Alamos National Security, LLC:
    LA-CC-15-039

    Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
    Copyright 2017. Los Alamos National Security, LLC. This software was produced
    under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
    Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
    the U.S. Department of Energy. The U.S. Government has rights to use,
    reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
    ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
    ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
    modified to produce derivative works, such modified software should be
    clearly marked, so as not to confuse it with the version available from
    LANL.

    THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
    THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
    ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
    OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
    IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
    OF SUCH DAMAGE.

Dependencies
============

There are a number of dependencies that should be installed before attempting to
build GUFI. There are also many optional dependencies that can enable
additional features.

System Tools
------------

+--------------------+------------+---------------------------------------+
| Package/Executable | Required?  | Comments                              |
+====================+============+=======================================+
| autotools          | :red:`Yes` | Some bundled packages use             |
|                    |            | autotools                             |
+--------------------+------------+---------------------------------------+
| C Compiler         | :red:`Yes` | C99 support                           |
+--------------------+------------+---------------------------------------+
| C++ Compiler       | No         | C++11 support - g++ 4.9.3,            |
|                    |            | clang++-3.9, or newer                 |
+--------------------+------------+---------------------------------------+
| CMake              | :red:`Yes` | Version 3.1 or higher                 |
+--------------------+------------+---------------------------------------+
| Make               | :red:`Yes` |                                       |
+--------------------+------------+---------------------------------------+
| Python             | :red:`Yes` | Python 2.7 or Python 3                |
+--------------------+------------+---------------------------------------+
| pip                | No         | Used for installing and client        |
|                    |            | dependencies                          |
+--------------------+------------+---------------------------------------+
| git                | No         | 1.8.5 or higher if using the          |
|                    |            | performance history framework         |
+--------------------+------------+---------------------------------------+
| ImageMagick        | No         | Only needed if building LaTeX         |
|                    |            | documentation                         |
+--------------------+------------+---------------------------------------+
| LaTeX              | No         | Only needed if building LaTeX         |
|                    |            | documentation                         |
+--------------------+------------+---------------------------------------+
| pkg-config         | :red:`Yes` |                                       |
+--------------------+------------+---------------------------------------+
| realpath           | :red:`Yes` |                                       |
+--------------------+------------+---------------------------------------+
| rpmbuild           | No         | Only needed if building RPMs          |
+--------------------+------------+---------------------------------------+
| setfattr           | :red:`Yes` | attr package                          |
|                    |            | ``xattr -w`` on macOS                 |
+--------------------+------------+---------------------------------------+
| sphinx             | No         | Only needed if building sphinx        |
|                    |            | documentation                         |
+--------------------+------------+---------------------------------------+
| truncate           | :red:`Yes` | | The -s option must be available     |
|                    |            | | Can be installed via brew /\w       |
|                    |            | | ``brew install coreutils`` on MacOS |
+--------------------+------------+---------------------------------------+
| uname              | No         | Only needed if building RPMs          |
+--------------------+------------+---------------------------------------+

Libraries
---------

+------------+-----------+-------------------------------------------+
| Name       | Required? | Comments                                  |
+============+===========+===========================================+
| xattr      | :red:`Yes`|                                           |
+------------+-----------+-------------------------------------------+
| pcre       | :red:`Yes`| Version 1                                 |
+------------+-----------+-------------------------------------------+
| fuse       | No        | Called osxfuse on macOS                   |
+------------+-----------+-------------------------------------------+
| db2        | No        |                                           |
+------------+-----------+-------------------------------------------+
| gpfs       | No        |                                           |
+------------+-----------+-------------------------------------------+
| matplotlib | No        | Python library used for plotting          |
|            |           | performance history data                  |
|            |           |                                           |
+------------+-----------+-------------------------------------------+
| zlib       | No        |                                           |
+------------+-----------+-------------------------------------------+

Packages provided by GUFI
-------------------------

Several packages are provided/downloaded, built, and installed
automatically:

+--------------+------------------------------------------------------+
| Name         | Comments                                             |
+==============+======================================================+
| GoogleTest   | https://github.com/google/googletest                 |
+--------------+------------------------------------------------------+
| jemalloc     | https://github.com/jemalloc/jemalloc                 |
+--------------+------------------------------------------------------+
| paramiko     | | https://github.com/paramiko/paramiko               |
|              | | patched ae3d0febef17a8ece5268bbf6c210a30573ce800   |
|              | | Use pip to download python-bcrypt,                 |
|              | | python-cryptography, and python-pynacl             |
+--------------+------------------------------------------------------+
| sqlite3      | | https://www.sqlite.org/index.html                  |
|              | | patched sqlite-autoconf-3270200.tar.gz             |
+--------------+------------------------------------------------------+
| sqlite3-pcre | https://github.com/mar-file-system/sqlite3-pcre      |
+--------------+------------------------------------------------------+
