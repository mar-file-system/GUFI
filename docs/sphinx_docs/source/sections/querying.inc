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

Querying
========

Once a GUFI tree with one or more indexes has been created, it should be
queried using GUFI tools and SQL statements.

``gufi_query``
--------------

``gufi_query`` is the main tool used for accessing indexes.

``gufi_query`` processes each directory in parallel, passing user
provided SQL statements to the current directory’s database. Because of
this, callers will need to know about GUFI’s database and table schemas.
We expect only “advanced” users such as administrators and developers to
use ``gufi_query`` directly. Users who do not know how GUFI functions
may call ``gufi_query``, but might not be able to use it properly.

Flags
~~~~~


  ``-h`` 
    Help                              

  ``-H``  
    Show assigned input values (debugging)                       

  ``-E <SQL ent>`` 
    SQL for entries table             

  ``-S <SQL sum>``
    SQL for summary table             

  ``-T <SQL tsum``  
    SQL for tree-summary table        

  ``-a``
    AND/OR (SQL query combination)    

  ``-n <threads>``
    Number of threads (default: 1)    

  ``-j``  
    Print the information in terse form                              

  ``-o <out_fname>``
    Output file (one-per thread, with thread-id suffix)                 

  ``-d <delim>`` 
    One char delimiter (default: ``\x1E``)                         

  ``-O <out_DB>``
   Output DB                         

  ``-I <SQL_init>`` 
   SQL init                          

  ``-F <SQL_fin>``
    SQL cleanup                       

  ``-y <min-level>`` 
    Minimum level to descend to       

  ``-z <max-level>``
    Maximum level to descend to       

  ``-J <SQL_interm>`` 
    SQL for intermediate results (no default. ex.: 
    <  ``INSERT INTO <  aggregate table name> SELECT * FROM < intermediate table name>``) 

  ``-K <create aggregate>``
    SQL to create the final aggregation table                 

  ``-G <SQL_aggregate>`` 
    SQL for aggregated results (no default. ex.:
    <  ``SELECT * FROM <aggregate table name>``) 

  ``-m`` 
    Keep mtime and atime same on the database files                    

  ``-B <buffer size>``
    Size of each thread’s output buffer in bytes                   

  ``-w``
    Open the database files in read-write mode instead of read
    only mode                         

  ``-x``
    Enable xattr processing           

  ``-k``
   File containing directory names to skip                           

  ``-M <bytes>``
   Target memory footprint           

  ``-e``
   Compress work items               

Flag/Table Associations
~~~~~~~~~~~~~~~~~~~~~~~

A ``gufi_query`` call should use at least one of the ``-T``, ``-S``, and
``-E`` flags to pass in SQL statements [1]_.

-  ``-T`` should be used to query the ``treesummary`` table.

-  ``-S`` should be used to query the ``summary`` table and its
   variants.

-  ``-E`` should be used to query the ``entries`` table and its
   variants.

Note that user provided SQL statements are passed directly into
SQLite [2]_ and thus associations between flags and tables are not
enforced [3]_.

Prior to rolling up, the ``pentries`` view can be treated as an optional
improvement to the ``entries`` table. After rolling up, the ``pentries``
view will have been updated extensively and will contain both the
original ``entries`` data as well as the rolled up data. Querying the
``entries`` table of a rolled up index will return a subset of the total
results that exist in the index since it only contains the current
directory’s information and subdirectories are not traversed. The
``summary`` table was updated directly, so querying it will return all
directory information.

However, instead of querying the ``summary`` table and switching between
querying ``entries`` and ``pentries``, the ``vrpentries`` and
``vrsummary`` views should always be used. These views contain data from
the ``summary`` table that allow for the path of each row relative to
the starting path to be generated using the ``rpath`` SQL function,
whether or not the index has been rolled up, with consistent input
arguments (see Table `5 <#tab:sqlwcontext>`__ for details). These views
and function were set up to help simpliy the queries that users provide.

.. [1]
   Not passing in any SQL statements results in ``gufi_query`` simply
   walking the tree and filling up the inode and dentries caches.

.. [2]
   Anything that can be done with SQL can also be done on the databases
   in an index. To prevent accidental modifications, databases default
   to opening in read-only mode.

.. [3]
   Querying tables/views with the wrong flags may result in unexpected
   output.

Short Circuiting with Compound Queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At each directory, the set of user provided SQL statements run on the
``db.db`` file in the following order: if ``-T`` was provided, it will run
if the optional ``treesummary`` table exists in the database. If ``-T``
was not run or returned at least one row of results, ``-S`` will run if
it was provided. If ``-S`` was not provided or returned at least one row
of results, ``-E`` will run if it was provided.

Conversely, if ``-T`` runs and does not return any results, processing
is stopped before running the ``-S`` query. If ``-S`` does not return
any results, the ``-E`` query is not run. This allows for threads to
short circuit the processing of a directory if it is determined early on
that no rows would be obtained from a later query. For example, if a
query is searching for files larger than 1MB, but the ``-S`` query found
that the range of file sizes is 1KB to 5KB, there is no need to run the
``-E`` query to get rows, since no rows will match in any case.

To turn off short circuiting and always run all queries for each
directory, pass the ``-a`` flag to ``gufi_query``.

.. .. _extended-attributes-1:

Extended Attributes
~~~~~~~~~~~~~~~~~~~

When querying for xattrs, pass ``-x`` to ``gufi_query`` to build the
``xattrs`` view for querying. This view is a SQL union of rolled in
xattrs and any external databases that successfully attaches. Attaching
the database files checks the permissions of the xattrs. ``UNION``
removes duplicate entries that showed up in multiple external databases,
leaving a unique set of xattrs accessible by the caller.

The ``xentries``, ``xpentries``, ``xsummary``, ``vrxpentries``, and
``vrxsummary`` views are convenience views generated so that users do
not have to perform joins themselves. They are ``entries``,
``pentries``, ``summary``, ``vrpentries``, and ``vrsummary`` enhanced
with the ``xattr_name`` and ``xattr_value`` columns. Like the ``xattrs``
view, these views are also dropped at the end of directory processing.

Note that entries with multiple extended attributes will return multiple
times in this view.

Aggregation
~~~~~~~~~~~

There are cases where independent per-thread results are not desirable,
such as when sorting or summing, where the results from querying the
index must be aggregated for final processing.

In order to handle these situations, the ``-I`` flag should be used to
create per-thread intermediate tables that are written to by ``-T``,
``-S``, and ``-E``. The intermediate table results will then be
aggregated using ``-J`` into the final table created by ``-K``. The rows
stored in the final table are processed one last time as a whole, rather
than as results from independent threads, using ``-G``.

Per-Thread Output Files
~~~~~~~~~~~~~~~~~~~~~~~

The ``-o`` flag causes results to be outputted to text files. When
outputting in parallel, per-thread output files are created with the
thread id appended to the filename provided with the flag. When
aggregating, the aggregate results are written to the filename specified
by ``-o`` with no filename modifications.

Per-Thread Output Database Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``-O`` flag allows for results to be written to SQLite database
files instead of text files. The resulting filenames follow the same
creation rules as ``-o``. However, the queries passed into
``gufi_query`` need modifications. When writing in parallel, ``-I`` is
needed to create the table for each per-thread output database. ``-T``,
``-S``, and ``-E`` should be modified to write to the per-thread tables
in the same way as writing to the intermediate tables when aggregating.

When writing aggregate results to a database, ``-G`` is not needed as
``-J`` already wrote the results into the aggregate table. However,
``-G`` may still be provided to get results during the ``gufi_query`` in
addition to queries on the results database file later on. Output 
database files may be passed to the ``querydbs`` executable for
further processing.

Example Calls
~~~~~~~~~~~~~

Each of these must be called with ``gufi_query /path/to/index``.                     


**Parallel Results**

*stdout*

.. code-block:: Bash

        -S "SELECT * FROM vrsummary;"           

*Per-thread Files*

.. code-block:: Bash

     -E "SELECT * FROM vrpentries;"       
     -o results                                         

*Per-thread Database*

.. code-block:: Bash

     -I "CREATE TABLE results(name TEXT, size INT64);"  
     -S "INSERT INTO results SELECT name, size FROM vrsummary;"
     -E "INSERT INTO results SELECT name, size FROM vrpentries;" 
     -O results                                         


**Aggregate Results**

*stdout*

.. code-block:: Bash

     -I "CREATE TABLE intermediate(name TEXT, size INT64);" 
     -S "INSERT INTO intermediate SELECT name, size FROM vrsummary;"
     -K "CREATE TABLE aggregate(name TEXT, size INT64);"   
     -J "INSERT INSERT aggregate SELECT * FROM intermediate;"     
     -G "SELECT * FROM aggregate ORDER BY size DESC;"       

*Single File*

.. code-block:: Bash

     -I "CREATE TABLE intermediate(name TEXT, size INT64);"
     -E "INSERT INTO intermediate SELECT name, size FROM vrpentries;" 
     -K "CREATE TABLE aggregate(name TEXT, size INT64);"
     -J "INSERT INSERT aggregate SELECT * FROM intermediate;"
     -G "SELECT * FROM aggregate ORDER BY size DESC;"
     -o results

*Single Database File*

.. code-block:: Bash

     -I "CREATE TABLE intermediate(name TEXT, size INT64);"     
     -S "INSERT INTO intermediate SELECT name, size FROM vrsummary;"    
     -E "INSERT INTO intermediate SELECT name, size FROM vrpentries;"     
     -K "CREATE TABLE aggregate(name TEXT, size INT64);"     
     -J "INSERT INSERT aggregate SELECT * FROM intermediate;"     
     -O results                                             


Behavior When Traversing Rolled Up Indexes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``gufi_query`` detects that a directory being processed has been
rolled up, the thread processing that directory does not enqueue work to
descend further down into the tree, as all of the data underneath the
current directory is available in the current directory. While this may
seem to only reduce tree traversal by “some" amount, in practise,
rolling up indexes of real filesystems reduces the number of directories
(and thus the bottlenecks listed in
Section `9.1.1 <#subsec:bottlenecks>`__) that need to be processed by
over 99%, significantly reducing index query time.

Queries are not expected to have to change too often when switching
between unmodified indexes and rolled up indexes. If they do, they
should not have to change by much.

Visualizing the Workflow
~~~~~~~~~~~~~~~~~~~~~~~~

.. figure:: images/gufi_query_main.png
   :width: 100.0%
   :align: center

   Workflow of ``gufi_query``

``processdir``
^^^^^^^^^^^^^^

The core of ``gufi_query`` is the ``processdir`` function. This is where
the -T, -S, and -E flags are processed. Multiple instances of this
function are run in parallel via the thread pool in order to quickly
traverse and process an index.

.. figure:: images/gufi_query_processdir.png
   :width: 100.0%
   :align: center

   Workflow of ``processdir``

``querydb``
^^^^^^^^^^^

The ``querydb`` macro is used to execute SQL statements and handle
errors.

.. figure:: images/querydb_macro.png
   :width: 100.0%
   :align: center

   ``querydb`` macro workflow

``querydbs``
------------

``querydbs`` attaches all database files it is given into a single view
for querying. All tables in the provided database files should have the
same name and schema. For tables called ``table``, the view combining
all tables will be named ``vtable``. This was intended to allow for easy
post-processing of ``gufi_query`` ``-O`` output database files.

``querydbs`` does not have a ``gufi_`` prefix because it will work for
any set of input databases with the same schema. For example,
``querydbs`` can also be used directly on individual database files from
an index.

Note that there is a hard limit of 125 databases that can be queried by
querydbs that is set by SQLite3.

SQLite Functions
----------------

Several convenience functions are added into each database instance
opened for querying.

These are SQLite functions that are available in ``gufi_query`` 
and ``querydbs``:

  ``uidtouser(uid)``
	Converts a UID to a user name

  ``gidtogroup(gid)``
	Converts a GID to a group name

  ``modetotxt(mode)``
	Converts numerical permission bits to text

  ``strftime(format, timestamp)``
	Replaces `SQLite’s custom strftime <https://www.sqlite.org/lang_datefunc.html>`_ with ``strftime(3)``


  ``blocksize(bytes, unit)``
    Converts a size to a the number of blocks of size :code:`unit`
    needed to store the first argument. :code:`unit` is the
    combination of at least one integer and/or a prefix and a suffix:

    Prefix: ``K``, ``M``, ``G``, ``T``, ``P``, ``E``

    Suffix (multiplier): *no suffix* (1024), ``B`` (1000), ``iB``
    1024)
    
    Return value is a string.
    
    Note that this function was implemented to replicate
    ``ls`` output and is meant for use with ``gufi_ls`` only,
    so use with caution.
    
    Examples:

    +------------------------+-------+
    | Call                   | Output|
    +========================+=======+
    | blocksize(1024, ‘1000’)| 2     |
    +------------------------+-------+
    | blocksize(1024, ‘1024’)| 1     |
    +------------------------+-------+
    | blocksize(1024, ‘K’)   | 1K    |
    +------------------------+-------+
    | blocksize(1024, ‘1K’)  | 1     |
    +------------------------+-------+
    | blocksize(1024, ‘KB’)  | 2KB   |
    +------------------------+-------+
    | blocksize(1024, ‘1KB’) | 2     |
    +------------------------+-------+
    | blocksize(1024, ‘KiB’) | 1KiB  |
    +------------------------+-------+
    | blocksize(1024, ‘1KiB’)| 1     |
    +------------------------+-------+

``human_readable_size(bytes)``
    Converts a size to a human readable string

These are SQLite functions that require the context of an index 
and thus are only available in ``gufi_query``:

 
 ``path()``                 
    Current directory relative to path passed into executable                  

 ``epath()``                
    Current directory basename              

 ``fpath()``                
    Full path of current directory          

 ``rpath(sname, sroll)``
    Current directory relative to path passed into executable                  
    taking into account the rolled up name in summary table                        
    and rollup score. Should only be used with the ``sname``                      
    and ``sroll`` columns of the ``vrpentries``, ``vrsummary``,          
    ``vrxpentries``, and ``vrxsummary`` views.                                  
                                                                    
    Usage:                                  
                                            
    .. code-block:: Bash

        SELECT rpath(sname, sroll) 
        FROM vrsummary vrxsummary;           

        SELECT rpath(sname, sroll) || "/" || name
        FROM vrpentries/vrxpentries;         


 ``level()``                
    Depth of the current directory from the starting directory                      

 ``starting_point()``       
    Path of the starting directory          

 ``subdirs_walked()``       
    Number of subdirectories that were enqueued while processing 
    the current directory. This function will return 0 if the                    
    current directory was rolled up. Only available in ``-S``                     
    and ``-E``. Not recommended for use with ``-E``.                            

``gufi_stat_bin``
-----------------

| ``gufi_stat`` is a script that can be used to extract individual
  entries from an index like ``stat(1)``. However, it is not a wrapper
  for ``gufi_query``. Instead, it is a wrapper for ``gufi_stat_bin``, a
  compiled executable. ``gufi_stat_bin`` does not use the configuration
  file and thus needs to be provided the search path.

Flags
~~~~~

  ``-f <FORMAT>`` 
    Use the specified FORMAT instead of the default. A newline is outputted after each use of FORMAT.

  ``-j``          
    Print the information in terse form; Same as ``-t``/``--terse`` in ``stat(1)``.














