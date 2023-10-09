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

Implementation Details
======================

In addition to glue code that combines the various well known
technologies used to implement GUFI, there exists in the codebase
several pieces of code, design decisions, and optimizations that have
made GUFI performant.

``QueuePerThreadPool``
----------------------

Thread pools are generally written with a single work thread (with a
single lock) from which to threads pull from. This does not work past a
handful of threads. GUFI can run on hundreds of threads, and thus
required a more performant thread pool. The solution to this was
``QueuePerThreadPool``.

``QueuePerThreadPool`` is named so because originally, there was one
work queue (and lock) maintained for each thread in the thread pool.
Now, there are multiple work queues maintained for each thread.

Adding Work
~~~~~~~~~~~

By default, work is added to threads in a round robin fashion in order
to distribute work evenly and to attempt to prevent, or at least reduce
the amount of, contention experienced by any one work queue.

This function can be changed during initialization.

Processing Queued Work
~~~~~~~~~~~~~~~~~~~~~~

Because work items are enqueued in parallel while work items are
processed, popping off work items one at a time results in one lock per
removal that might experience contention while the queue is being
modified. In ``QueuePerThreadPool``, when a thread pops off work for
processing, **all** work items in the queue are removed, resulting in
the removal of multiple work items with only a single lock that might
experience contention. All work is processed before the thread returns
to the work queue to find more work.

There exists a second work queue, called the deferred work queue, that
is pushed to if the thread pool is initialized with a non-zero
``queue_limit``. Work items are placed in the deferred work queue when
the normal work queue has more than ``queue_limit`` items enqueued. The
deferred work queue is only processed if the work queue is empty when
the worker thread goes to look for more work. The work queue may still
be pushed to if it was recently moved for processing (since the work
queue now has as size of 0). This changes the order that work is
processed, allowing for work to drain, reducing memory pressure, while
still continuously processing work when there is work present.

If a thread discovers that it does not have work items in either queue
but the thread pool still has outstanding work, it will search for more
work in other threads. If work items are found, the thread will steal
some of them, causing the stolen work items to experience less latency
between enqueuing and processing. Note that when work items are stolen,
the work queue is searched first, and if nothing is found, the deferred
queue is searched next, instead of the next thread’s work queue.

.. _usage-4:

Usage
~~~~~

#. Create a thread pool:

   ``QPTPool_t *pool = QPTPool_init(nthreads, args);``

   ``nthreads`` sets the number of threads in this thread pool.

   The ``args`` argument will be accessible by all threads that are run.

#. Setting Properties:

   ``QPTPool_init`` is intentionally kept simple and uses default values
   for some features. These values may be modified using
   ``QPTPool_init_with_props`` or ``QPTPool_set_*`` functions before
   ``QPTPool_start`` is called.

   By default, ``QueuePerThreadPool`` will push new work items in a
   round robin fashion. This can be changed to a custom function with
   ``QPTPool_set_next``. This function is set at the context level
   instead of at ``QPTPool_enqueue`` in order to not require a branch
   to figure out whether or not the provided function pointer is
   valid.

   ``QPTPool_set_queue_limit`` causes work items to be pushed into the
   deferred work queue as described above.

   ``QPTPool_set_steal`` sets the numerator and denominator of the
   multiplier used when work items are being stolen from other threads.
   For the first queue where ``queue.size * numerator / denominator``
   results in at least 1 work item, that many work items will be taken
   from the front of the queue.

#. Getting Properties:

   Properties may be extracted from the context using the
   ``QPTPool_get_*`` functions.

#. Start the thread pool:

   ``QPTPool_start(pool);``

#. Add work:

   ``QPTPool_enqueue(pool, id, function, work);``

   The function passed into ``QPTPool_enqueue`` must match the signature
   found in ``QueuePerThreadPool.h``. The ``work`` argument will only be
   accessible to the thread processing this work.

   The thread that will receive the new work item is not ``id``. Rather,
   ``id`` is treated as the source thread id and
   ``threads[id]->next_queue`` will be where the new work item is
   enqueued.

#. Wait for all work to be completed (threads are joined):

   ``QPTPool_wait(pool);``

   This function exists to allow for the collection of statistics before
   the context is destroyed.

#. Destroy the pool context:

   ``QPTPool_destroy(pool);``

.. _`sec:bottomup`:

``BottomUp``
------------

``BottomUp`` performs parallel downward directory traversal followed by
parallel upward directory traversal only after every single child of a
directory has been traversed. Users pass in functions to run when
descending and ascending and have access to ``lstat(2)`` data collected
during the tree traversal.

Note that ``BottomUp`` cannot be used for normal GUFI operations. GUFI
expects each directory to be independently processed - directories will
always start processing before their children, but might not complete
processing until after their children have completed (the thread might
have been interrupted). ``BottomUp`` does not enqueue children until the
directory has been processed by the user provided descend function.
Ascent only occurs after all children have been processed, allowing for
the directory to see any changes that occurred under it when the user
provided ascend runs.

``BottomUp`` is used primarily for three executables: ``gufi_rollup``,
``gufi_treesummary_all``, and ``parallel_rmr``. They all require child
directories to be processed and modified before processing a
directory.

Printing to ``stdout``
----------------------

Printing in parallel to a single target such as ``stdout`` without
locking results in a jumble of text. Additionally, printing incurs a
base cost in addition to the amount of data being printed. The
OutputBuffers code, in conjunction with the ``print_parallel`` function,
allows for data to be placed into individual buffers of known,
customizable sizes before being written to ``stdout`` in a single call
when a buffer is full.

The ``print_parallel`` function works as follows:

#. The size of the line to be printed, including the column delimiters
   and newline, is calculated.

#. If the line can fit into the provided buffer, write it into the
   buffer.

#. If the line cannot fit into the provided buffer, lock the print
   mutex, flush the buffered data, and then print the line. This
   maintains the ordering of the data outputted by the current thread.

Optimizations
-------------

In order for GUFI to be performant, many optimizations were used and
implemented.

Reduced Branching
~~~~~~~~~~~~~~~~~

In order to reduce the number of failed branch predictions experienced
by GUFI, branching was removed where possible. The main way this was
done was by intentionally skipping ``NULL`` pointer checks that are
repeated or always expected to be valid.

Allocations
~~~~~~~~~~~

Dynamic allocations are more costly to make than static allocations. To
reduce the amount of dynamic allocations, C-string are usually declared
as fixed size arrays instead of pointers.

During descent, each work item is initially allocated on the stack since
there is no way to tell beforehand whether or not the path pointed to by
a directory entry is a directory. If the path is a directory to be
enqueued, the work item is copied into dynamically allocated memory
first. If not, the work item allocated on the stack is used.

Additionally, allocations are not performed by the standard malloc.
Instead, ``jemalloc(3)`` is used to override ``malloc(3)``. See
`jemalloc’s website <https://jemalloc.net/>`__ for details.

Not Calling ``lstat(2)`` During Tree Walk
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``struct dirent``s are returned when reading a directory with
``readdir(3)``. glibc’s implementation of ``struct dirent`` provides
extra fields not required by POSIX.1. GUFI takes advantage of the
nonstandard ``d_type`` field to not call ``lstat(2)`` when determining
whether or not the entry is a directory. This also prevents memory
allocations of work items that end up not being enqueued for processing.

Enqueuing Work Before Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to reduce the amount of time the thread pool spends waiting for
work, work is enqueued before doing the main processing for a directory.
When walking a tree, such as with ``gufi_dir2index``,
``gufi_dir2trace``, and ``gufi_query``, subdirectories are enqueued
before processing source filesystem information and running queries.
``gufi_trace2index`` reads each trace file using a single thread.
However, each stanza is not processed as it is encountered. Instead,
each stanza’s file offset is enqueued in the thread pool, allowing for
the directories of the index to be generated in parallel.

String Manipulations
~~~~~~~~~~~~~~~~~~~~

Where possible, strings are not copied, and are instead referenced.
Additionally, calls to ``strlen(3)`` are avoided in order to not walk
memory one byte at a time. Some string lengths, such as those for input
arguments are obtained with ``strlen(3)``. Afterwards, string
manipulations are done by using or modifying lengths to offset into or
cut short strings.

Combining Strings with ``memcpy(3)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One method of combining C-strings is by concantenating them with
``snprintf(3)`` with format strings containing only ``%s`` format
specifiers. Instead of parsing the format string, the ``SNFORMAT_S``
function was created to do ``memcpy(3)``\ s on the arguments, skipping
figuring out whether or not inputs are strings and how long they are by
finding ``NULL`` terminators. Instead, lengths are obtained as
by-products of previous string manipulations and the values are reused.

In-Situ Processing
~~~~~~~~~~~~~~~~~~

Occassionally, GUFI might encounter extremely large directories. This
results in many long lived dynamic allocations being created during
descent, which can overwhelm memory. Users can set a subdirectory limit
so that if too many subdirectories are encountered within a single
directory, subdirectories past the user provided count will be processed
recursively using a work item allocated on the thread’s stack instead of
being dynamically allocated and enqueued for processing. This reduces
memory pressure by limiting the amount of work items that extremely
large directories would otherwise spawn. The subdirectories being
processed recursively may themselves enqueue dynamically allocated
subdirectory work or recurse further down with subdirectory work
allocated on the stack.

Smaller Enqueued Work Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The main data structure that is enqueued is ``struct work``. This struct
was approximately 14KiB in size prior to
`2227d00 <https://github.com/mar-file-system/GUFI/commit/2227d00665eb6d507ac2052e80616c077a5da853>`__.
After moving the parts of this structure that were not necssary for
directory tree traversal to ``struct entry_data``, ``struct work`` was
reduced to slightly over 8KiB.

This was used to fix `Issue
121 <https://github.com/mar-file-system/GUFI/issues/121>`__.

Compression with zlib
~~~~~~~~~~~~~~~~~~~~~

When zlib is detected during CMake configuration, ``struct work`` can be
compressed to further reduce the size of each work item that is sitting
in memory waiting to be processed. The compressed buffer, originally
allocated with ``sizeof(struct work))`` bytes, is then reallocated to
the compressed size. The bulk of ``struct work`` is made up of text
strings followed by ``NULL`` characters, both of which are highly
compressible, meaning that compressed work items can be expected to be
much smaller than uncompressed work items.

Note that ``struct work`` is its own compressed buffer. Whether or not
the work item is compressed and the compressed length are now the first
two fields of ``struct work``. When a work item is compressed, a pointer
pointing to it will have less space allocated to it than
``sizeof(struct work))``.

This was used to fix `Issue
121 <https://github.com/mar-file-system/GUFI/issues/121>`__.

Database Templates
~~~~~~~~~~~~~~~~~~

Every directory in an index contains at least one database file, called
db.db, containing the ``lstat(2)`` data from the source filesystem. When
creating indexes, a database file is created with the same schema as
db.db and is left unfilled. When each directory in the index is
processed, the database file created earlier is copied into the
directory as a bytestream instead of having SQlite open new database
files for each directory. This avoids the multitudes of checks done by
SQLite when setting up databases and tables. The same is done for
external xattr database files.

SQLite
~~~~~~

As SQLite is a major component in GUFI, attempts were made to optimize
its usage. Some optimizations were made at compile time. See the `SQLite
Compile-time Options <https://www.sqlite.org/compile.html>`__ page for
details.

Locking
^^^^^^^

In order to prevent multiple threads from corrupting data, SQLite
implements locking. In GUFI, each database is only ever accessed by one
thread:

-  When indexing, only one thread writes to each directory’s database.

-  When querying, the per-thread results are written to per-thread
   databases. After the tree walk, the per-thread databases are merged
   serially into a final database.

Locking despite never modifying databases in parallel is not useful, and
was removed by setting ``-DSQLITE_THREADSAFE=0`` in the compile flags.

VFS
^^^

In addition to not locking SQLite in-memory operations, locking at the
filesystem level was also disabled. Instead of opening SQLite database
files with the default VFS, GUFI uses the ``unix-none`` VFS, which
causes all file locking to become no-ops. See `The SQLite OS Interface
or “VFS” <https://www.sqlite.org/vfs.html>`__ for details.

Memory Tracking
^^^^^^^^^^^^^^^

| Memory tracking was disabled with
| ``-DSQLITE_DEFAULT_MEMSTATUS=0``.

Temporary Files
^^^^^^^^^^^^^^^

| Temporary files can be stored to disk or in memory. GUFI forces all
  temporary files to be stored in memory with
| ``-DSQLITE_TEMP_STORE=3``.

Caching Queries (Not Merged)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When queries are performed on indexes, they are processed from scratch
by each thread for each directory. An obvious optimization would be to
reduce the amount of string parsing and query planning by compiling each
query once (or a small number of times such as once per thread) at the
beginning of a run and saving the compiled queries for repeated use
during the index traversal.

An attempt at caching queries was made with `Pull Request
#95 <https://github.com/mar-file-system/GUFI/pull/95>`__. Unfortunately,
caching queries at best seemed to perform on par with the latest GUFI
and at worst, slightly slower than the latest GUFI. This was true for
both simple queries and complex queries with ``JOIN``\ s.
