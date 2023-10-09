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

Indexing
========

The first step to using GUFI is indexing a source filesystem.

An index created by GUFI retains the shape of the source filesystem:
directories that exist in the source filesystem also exist in the index.
If an administrator created the index, the directories will also have
the same access permissions, uid, and gid.

Indexes will not contain any of the files that the source filesystem
contained. Instead, all metadata extracted from a single directory of
the source filesystem will be placed into a single database file, called
db.db, in the cooresponding directory in the index. Each database file
will be created with a fixed schema that includes the tables listed in
Sections `7 <#sec:schema>`__ and `8.3.2 <#sec:xattr_schema>`__.
Additional database files may be created if extended attributes are
extracted (see Section `8.3 <#sec:xattrs>`__).

Index processing (creation) occurs on a per-directory basis, and thus is
highly parallelizable.

Directly Indexing a Filesystem
------------------------------

``gufi_dir2index``
~~~~~~~~~~~~~~~~~~

``gufi_dir2index`` is used to directly create an index based off of the
contents of a provided directory.

Usage
^^^^^


``gufi_dir2index [flags] input_dir... output_dir``

The index of ``input_dir``:math:`_i` will be placed in
``output_dir/$(basename input_dir)``:math:`_i`.

Flags
^^^^^

  ``-h``               
    help manual

  ``-H``               
    Show assigned input values

  ``-n <num_threads>`` 
    Define number of threads to use

  ``-x``               
    Pull xattrs from source file-sys into GUFI

  ``-z <max_level>``   
    Maximum level to go down to

  ``-k <filename>``    '
    File containing directory names to skip

  ``-M <bytes>``       
    Target memory footprint

  ``-C <count>``       
    Number of subdirectories allowed to be
    enqueued for parallel processing. Any
    remainders will be processed in-situ

  ``-e`` 
    Compress work items


.. figure:: images/gufi_dir2index.png
   :width: 100.0%
   :align: center

   ``gufi_dir2index`` workflow


Indirectly Indexing a Filesystem
--------------------------------

Trace Files
~~~~~~~~~~~

Traces files, or flat files, are text files containing ``lstat(2)``
information pulled from source filesystems separated by delimiters.
Trace files are generated by ``gufi_dir2trace`` and are processed by
``gufi_trace2index``.

The default delimiter is the ASCII Record Separator character ``\x1E``,
but can be changed to any 8-bit character. Characters that can appear in
filesystems should not be used as the delimiter in order to not confuse
``gufi_trace2index``.

The metadata pulled from each directory is represented by a “stanza” in
a trace file. A stanza starts with a line of directory metadata followed
by zero or more lines of regular file and symbolic link metadata. Note
that the summary of each directory is not stored in trace files and are
instead generated when the index is built.

Due to the parallelism of indexing, stanzas can appear in any order
within a trace file and can be placed in any of the per thread trace
files that are generated. Traces files can be concatenated together if
doing so is necessary or more convenient.

``gufi_dir2trace``
~~~~~~~~~~~~~~~~~~

``gufi_dir2trace`` generates trace files to allow for indexes to be
easily transfered to different locations rather than requiring entire
trees to be copied around.

Usage
^^^^^

``gufi_dir2trace [flags] input_dir... output_prefix``

Trace files with the name ``output_prefix.${i}``, where i :math:`\in`
[0, number of threads), will be created.


Flags 
^^^^^

  ``-h``               
    Help manual

  ```-H``               
    Show assigned input values

  ``-n <num_threads>`` 
    Define number of threads to use

  ``-x``               
    Pull xattrs from source file-sys into GUFI

  ``-d <delim>``       
    Delimiter (one char) [use ’x’ for 0x1E]

  ``-k <filename>``    
    File containing directory names to skip

  ``-M <bytes>``       
    Target memory footprint

  ``-C <count>``       
    Number of subdirectories allowed to be enqueued for parallel processing. 
    Any remainders will be processed in-situ.

  ``-e``               
    Compress work items



``gufi_trace2index``
~~~~~~~~~~~~~~~~~~~~

``gufi_trace2index`` is used to convert trace files into indexes. It is
essentially the same as ``gufi_dir2index`` except it obtains data from
trace files instead of the filesystem being indexed.

Per thread trace files may be passed into ``gufi_trace2index`` directly.
Note that passing in too many trace files at once might result in
running out of file descriptors. Alternatively, the per thread trace
files may be concatenated in any order into a smaller number of larger
files for processing.

Extended attributes will be processed if they are found in the traces.
There is no need to tell ``gufi_trace2index`` to process them with
``-x``.

Usage
^^^^^
  
``gufi_trace2index [flags] trace_file... index_root``

Each source filesystem found in the trace files will be converted to
an index placed underneath ``output_dir``.

Flags
^^^^^

      ``-h``               
        Help manual

      ``-H``               
        Show assigned input values

      ``-n  <num_threads>``
        Define number of threads to use

      ``-d <delim>``    
        Delimiter (one char) [use ’x’ for 0x1E]

      ``-M`` <bytes>``          
        Target memory footprint


Extended Attributes
-------------------

GUFI supports the indexing and querying of extended attributes (xattrs).

Reading standard filesystem permissions of files only requires read (and
execute) access to the directory. Extended attribute names are visible
this way. However, because xattr values are user defined data, their
permissions are checked at the file level, requiring changes to how GUFI
stores data. For more information on xattrs, see ``xattr(7)``,
``llistxattr(2)``, and ``lgetxattr(2)``.

Directories containing files from multiple users might have xattrs that
are not readable by all who can view the ``lstat(2)`` data of the
directory. As GUFI was originally designed to only use directory-level
permission checks, a number of modifications were made to process xattrs
without violating their permissions.

Roll In
~~~~~~~

Extended attributes that are readable by all who have access to the
directory are stored in the ``xattrs_pwd`` table in the main database.
These xattrs are referred to as “rolled in”.

The rules that determine whether or not an xattr pair can roll in are as
follows:

-  File is 0+R

-  File is UG+R doesnt matter on other, with file and parent same usr
   and grp and parent has only UG+R with no other read

-  File is U+R doesnt matter on grp and other, with file and parent same
   usr and parent dir has only U+R, no grp and other read

-  Directory has write for every read: ``drw*rw*rw*`` or ``drw*rw*___``
   or ``drw*______`` - if you can write the dir you can chmod the files
   to see the xattrs

Extended attributes that cannot be read by all who can read the
directory are stored in external per-uid and per-gid databases set with
``uid:nobody`` and ``nobody:gid`` owners respectively. This makes it so
that non-admin users cannot access the xattrs stored in external
databases that they do not have permissions to access.


Schema
~~~~~~

The main database and all external databases contain the following
tables and views with the 3 columns ``inode``, ``name``, and ``value``:

-  The ``xattrs_pwd`` table contains all extended attributes of the
   current directory that were placed into this database file.

-  The ``xattrs_rollup`` table contains all extended attributes that
   were placed into the children directories that were subsequently
   rolled up into the current directory.

-  The ``xattrs_avail`` view is the union of all extended attributes in
   ``xattrs_pwd`` and ``xattrs_rollup`` in this database file.

Additionally, each main database file has the following tables and views
in order to keep track of which files were created by GUFI for the
purposes of storing xattrs:

-  The ``xattrs_files_pwd`` table contains a listing of external
   database filenames that contain xattrs that were not rolled in.

-  The ``xattrs_files_rollup`` table contains a listing of external
   database filenames that contain xattrs that were not rolled in, but
   were brought in by rolling up.

-  The ``xattrs_files`` view combines the listings of database filenames
   found in ``xattrs_files_pwd`` and ``xattrs_files_rollup``.


Usage
~~~~~

Extended attributes are not pulled from the filesystem by default. In
order to pull them, pass ``-x`` to ``gufi_dir2index`` or
``gufi_dir2trace``.

Note that only xattr pairs in the user namespace (``user.*``) are
extracted.

Location
--------

GUFI is expected to be used to query the indexes of many filesystems all
at once. However, the source filesystems are not expected to be
accessible from each other. In order for the indexes from many
disconnected systems to be queried at once, they should all be built
under a common directory on a single machine.

Permissions
~~~~~~~~~~~

If a GUFI tree is located on a different machine than the source tree,
the users and groups are probably not available on the machine with the
index on it. ``/etc/passwd`` should be populated with the entries from
the source machine and modified so that they do not have a home
directory, and are not allowed to log in (``/sbin/nologin``). Similarly,
``/etc/group`` should be updated with the source machine’s groups and
modified to remove any unnecessary information.
