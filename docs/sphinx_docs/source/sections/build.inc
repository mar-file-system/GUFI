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

Build
=====

GUFI uses CMake to generate Makefiles. CMake 3.1 or higher is required.
From the root directory of the GUFI source, run:

::

   mkdir build
   cd build
   cmake .. [options]
   make

The recommended options for deployment are
``-DCMAKE_BUILD_TYPE=Release -DCLIENT=On``.


GUFI is known to build on Ubuntu 16.04 (Xenial), Ubuntu 18.04
(Bionic), Ubuntu 20.04 (Focal), Ubuntu 22.04 (Jammy), CentOS 7,
CentOS 8, RockyLinux 8, OSX 10.13 (High Sierra), OSX 11 (Big Sur),
OSX 12 (Monterey), and OSX 13 (Ventura). Building on Windows with
cygwin, GCC (not MinGW), and with jemalloc turned off also works.
OpenSUSE 12.3 was supported in the past and may still work.

Environment Variables
---------------------

  ``CXX=false`` 
    Disable building of C++ code

CMake Flags
-----------

All CMake flags must be prepended with ``-D``

General
~~~~~~~

  ``-DCMAKE_INSTALL_PREFIX=<PATH>`` 
    Install to a custom directory when running ``make install``     

  ``-DCMAKE_BUILD_TYPE=Debug`` 
    Build with warnings and debugging symbols turned on                 

.. .. _dependencies-admin:

Dependencies Flags
~~~~~~~~~~~~~~~~~~

 ``-DDEP_DOWNLOAD_PREFIX=<PATH>``
    Location of downloaded dependencies. If the expected 
    files are found, they will not be downloaded. The default path  
    points to the bundled dependencies.                    

 ``-DDEP_BUILD_DIR_PREFIX=<PATH>``   
    Location to build dependencies. Defaults to                      
    ``${CMAKE_BINARY_DIR}/builds``.      

 ``-DDEP_INSTALL_PREFIX=<PATH>``     
    Location to install the dependencies. Defaults to
    ``${CMAKE_BINARY_DIR}/deps``. If the dependencies are not installed   
    in ``${CMAKE_BINARY_DIR}``, they will not need to be redownloaded, rebuilt, or 
    reinstalled everytime ``${CMAKE_BINARY_DIR}`` is deleted.  

 ``-DDEP_PATCH_SQLITE3_OPEN=<On|Off>``  
    Whether or not to patch SQLite3  open                             

 ``-DDEP_USE_JEMALLOC=<On|Off>``     
    Whether or not to build and link with jemalloc                    

Debug
~~~~~

``CMAKE_BUILD_TYPE`` must be set to ``Debug`` for these to have effect.

 ``-DPRINT_CUMULATIVE_TIMES=<On|Off>``  
    Print cumulative statistics at the end of some executables    

 ``-DPRINT_PER_THREAD_STATS=<On|Off>``  
   Print ``gufi_query`` event timestamps                      

 ``-DPRINT_QPTPOOL_QUEUE_SIZE=<On|Off>``  
    Print size of work queues every time a work queue receives a new 
    item or pops items off          

 ``-DGPROF=<On|Off>``                
    Compile with the ``-pg`` flag.   

Client
~~~~~~

 ``-DCLIENT=<On|Off>``
   Whether or not to install paramiko and gufi_client when ``make install`` is called

Testing
~~~~~~~

``-DTEST_WORKING_DIRECTORY=<PATH>`` 
    Directory to run tests in. Defaults to ``${CMAKE_BINARY_DIR}/test``

Do not turn these off unless you know what you are doing:

 ``-DRUN_ADDQUERYFUNCS=<On|Off>``  
    Compile ``gufi_query`` without calls to ``addqueryfuncs``        

 ``-DRUN_OPENDB=<On|Off>``         
    Compile ``gufi_query`` without calls to ``opendb``               

 ``-DRUN_SQL_EXEC=<On|Off>``       
    Compile ``gufi_query`` without calls to ``sqlite3_exec``        

Docs
~~~~

 ``-DLATEX_BUILD=<On|Off>``
    Whether or not to build PDF documentation when LaTeX is found         
