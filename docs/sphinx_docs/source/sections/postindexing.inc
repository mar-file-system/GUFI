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

Post-Indexing
=============

After indexing, additional operations can be performed on the index in
order to increase query performance.


Rollup
------

Rolling up is a major optimization that can reduce the number of
directories that need to be opened when querying by significant amounts
while still obtaining the same results as an unmodified index.

.. .. _`subsec:bottlenecks`:

Bottlenecks
~~~~~~~~~~~

During querying, every single directory runs a fixed set of operations:

-  ``opendir(3)``

-  ``readdir(3)`` (Looped)

-  ``sqlite3_open_v2``

-  ``sqlite3_exec_v2``

-  ``sqlite3_close(3)``

-  ``closedir(3)``

Each of these operations have costs associated with them. Some are fixed
and some depend on the shape and contents of the index. Reducing the
number of directories processed allows for fixed costs to be amortized.

.. .. _`sec:rolluprules`:

Rules
~~~~~

Whether or not a directory *CAN* roll up
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

|  
| In order for a child directory to be allowed to roll up into its
  parent directory, it must follow two rules. First, all of its children
  must be rolled up. Second, all of its children must have permissions
  that satisfy any one of the following conditions with the parent.

-  World readable and executable (o+rx)

-  Matching user, group, and others permissions, with the same user and
   group

-  Matching user and group permissions, readable and executable (ug+rx)
   with the same user and group, and not world readable and excutable
   (o-rx)

-  Matching user permissions, readable and executable (u+rx) with the
   same user and not group or world readable and executable (go-rx)

Note that because leaf directories have no children with which to have
conflicting permission with, they are considered rolled up.

Whether or not a directory *SHOULD* roll up
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

|  
| In addition to the permission-based rules that determine whether or
  not a directory can roll up, we also have a small check to say whether
  or not a directory should roll up.

As data is rolled upwards in the index, databases towards the top will
accumulate more and more data from its subtrees. If directories are
allowed to roll up without limit, some databases will become
significantly larger than others, causing large amounts of tail latency.

The ``gufi_rollup`` executable has the ``-L`` flag that limits the
number of entries that may be found within a single directory.

Steps
~~~~~

The processing of rolling up involves a number of steps that update the
tables of the parent directory.

First, the target directory is checked to ensure it can and should roll
up its children into itself using the rules listed in
:ref:`Rules`. If rollup will proceed, the parent’s
``summary`` table is updated with a rollup score of 1.

Then, the contents of each child is copied into the parent:

#. The child’s ``pentries`` view is copied into the parent’s
   ``pentries_rollup`` table. The parent’s ``pentries`` view is updated
   automatically.

#. The child’s ``summary`` table is copied into the parent’s ``summary``
   table with the names of each child directory prefixed with the
   parent’s name.

Rolling up can be viewed as flattening the contents of the index while
taking into consideration the permissions of each directory.

Rolling up will cause the size of the index to grow significantly due to
the amount of data being replicated. One obvious optimization would be
to only roll up to the top-most level where a directory can roll up to
(one extra copy of the subtree instead of repeated copies). This
however, will only allow for queries to take advantage of rollups if
they start above the top-most rollup directory. Starting a query below
the top-most rollup level would result in the original subtree’s query
time whereas the implemented method of rolling up allows for queries
starting at any point in the index to take advantage of rollups.

The rollup operation can take some time, and so indexes are not rolled
up automatically. The ``gufi_rollup`` executable must be called
manually.

Extended Attributes
~~~~~~~~~~~~~~~~~~~

Additional steps are needed to rollup xattrs:

-  The child’s ``xattrs_avail`` view (without external database data) is
   copied into the parent’s ``xattrs_rollup`` table

-  The child’s ``xattrs_files`` view (without external databases data)
   is copied into the parent’s ``xattrs_files_rollup`` table

-  The child’s external database files are copied into the parent. If
   the parent already has an external database file with the same uid or
   gid, the contents of the external database are copied into the
   parent’s external database’s ``xattrs_rollup`` table instead.

``gufi_rollup`` executable
~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to apply rollup to an index, run the ``gufi_rollup``
executable:

``gufi_rollup index_root``

.. _rollup_fig:
.. figure:: images/rollup.png
    :width: 100.0%
    :align: center

    Rollup

.. _rollup_function:
.. figure:: images/rollup_function.png
    :width: 100.0%
    :align: center

    Rollup Function Diagram


:numref:`rollup_fig` shows the
overall structure of the ``gufi_rollup`` executable. ``gufi_rollup``
recursively descends the index and performs the rollup operation on a
directory once all of the directory’s children have been processed as
shown in :numref:`rollup_function`.


Undoing a rollup
~~~~~~~~~~~~~~~~

To remove rollup data from an index, run the ``gufi_unrollup``
executable:
``gufi_unrollup index_root``

Generate :code:`treesummary` Tables
-----------------------------------

The ``treesummary`` table is an optional table that is placed into
``db.db``. It contains a summary of the entire subtree starting at current
directory using minimums, maximums, and totals of numerical values.

When a query is provided to ``gufi_query``\ ``-T``, the ``treesummary``
table is queried first. Because ``treesummary`` tables do not
necessarily exist, ``gufi_query`` first checks for the existence of the
``treesummary`` table in the directory being processed before performing
the ``-T`` query. If the ``-T`` query returns no results, the entire
subtree will be skipped.

``gufi_treesummary``
~~~~~~~~~~~~~~~~~~~~

Starting from a directory provided in the command line,
``gufi_treesummary`` recursively traverses to the bottom of the tree,
collecting data from the ``summary`` table of each child directory
database. If a ``treesummary`` table is discovered in a subdirectory,
descent down the tree is stopped as the ``treesummary`` table contains
all of the information about that directory as well as all
subdirectories underneath it. Once all of the data has been collected,
it is summarized and placed into the ``treesummary`` table of the
starting directory.

Generating ``treesummary`` tables for all directories using this
top-down approach will take a long time due to the repeated traversals
across the same directories. Because of this, ``gufi_treesummary``
generates the ``treesummary`` table for the provided directory only.

If generating ``treesummary`` tables using ``gufi_treesummary``, the
tables should be generated at optimal points within the index. For
example, if the index is on a home directory, it may be useful to
generate ``treesummary`` tables at each user’s home directory.

Example Call:

.. code-block:: Bash

    gufi_treesummary index_root

``gufi_treesummary_all``
~~~~~~~~~~~~~~~~~~~~~~~~

``gufi_treesummary_all`` generates ``treesummary`` tables for all
directories. This is done by walking to the bottom of the tree and
generating ``treesummary`` tables while walking back up, which only
occurs after all subdirectories have had their ``treesummary`` tables
generated. Leaf directories, by definition, do not have
subdirectories, and further traversal down is unnecessary and
impossible. Their ``treesummary`` tables are thus duplicates of their
``summary`` tables, providing the base case for the walk back up the
tree. Directories above the leaves can then use the ``treesummary``
data found in their immediate subdirectories, which are 1) guaranteed
to exist and 2) guaranteed to summarize the entire subdirectory’s
subtree, to generate their own ``treesummary`` tables.

Example Call:

.. code-block:: Bash

    gufi_treesummary_all index_root

``gufi_rollup``
~~~~~~~~~~~~~~~

Just as with ``gufi_treesummary_all``, rolling up a tree involves
walking to the bottom of the tree and working upwards. This allows for
``treesummary`` generation to be performed automatically during the roll
up operation, resulting in ``treesummary`` tables being generated for
all directories whether or not they were rolled up.
