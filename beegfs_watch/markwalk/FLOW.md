WHAT THIS PROGRAM DOES
======================

BeeGFS Watch tells us "this path changed".

GUFI's incremental update does not take paths. It takes a file of inode
numbers, one per line, and rescans exactly those directories.

This program sits between the two.

    Watch  --events-->  us  --suspect file-->  gufi_incremental_update


THE FLOW
========

1. Watch dials in to us. We are the server, it is the client.

2. For each event, work out which directories are now out of date.
   Put them in a set. Do nothing else.

3. Every 5 seconds, take the set, turn it into inode numbers,
   and write the suspect file.

4. Print the gufi_incremental_update command to run with that file.

Nothing heavy happens per event. A hundred writes to one directory
become one entry in the set.


STEP 2: WHICH DIRECTORIES DOES AN EVENT DIRTY?
==============================================

Listing changes (CREATE, MKNOD, SYMLINK, UNLINK, RMDIR): the parent.
The listing lives in the parent.

Inode changes (SETATTR, TRUNCATE, FLUSH, CLOSE_WRITE, LAST_WRITER_CLOSED,
STRIPE_PATTERN_CHANGED, tracked OPEN_*): the path itself. The walk
(step 3) keeps it if it is a directory, since a directory's own mode,
owner and times live in its own db.db. Otherwise it rises to the parent,
where a file's are recorded. The root's path is "/", which means the
mount, never the mount's parent.

The exceptions:

  MKDIR         parent AND the new directory
                Marking only the parent creates the directory in the
                index with no database, so its contents never appear.

  RENAME        both parents AND their common ancestor
                Two parents alone make GUFI see a delete plus a create.
                It then throws the moved databases away instead of
                moving them.

  HARDLINK      both parents
                Link counts are stored per name, so the directory
                holding the other name is now stale too.

  OPEN_BLOCKED  nothing. The open was refused, nothing changed.

  INODE_LOCKED  nothing. Its path field is a bare filename, not a path.

  OPEN_*        nothing, unless --track-opens is set.
                They only move atime, and there are a lot of them.

  INVALID       nothing. It carries no path.


STEP 2b: FILES WITH MORE THAN ONE NAME
======================================

A write changes the inode. Every name of that inode now reports a new
size and time. The event only tells us about one name.

So: if the event touched the inode AND another name exists, set the
event's entry ID aside. At flush time we ask the index, in one query, where
else that inode is listed, and mark those directories too.

The entry ID comes straight off the event. Hard links share it, and
the BeeGFS index plugin records it in a beegfs_entries table, so the
lookup needs no stat and does not care how the client computes inode
numbers. It also still works for a name that was unlinked between the
event and the batch.

Checked in that order, so a normal single-link file costs one integer
comparison and nothing else. No query runs if nothing qualifies.

"Another name exists" is num_links > 1 for every event except UNLINK.
Measured on v8: UNLINK reports the names LEFT (three names: rm -> 2,
rm -> 1, last rm -> none). So UNLINK qualifies at num_links >= 1: the
inode's link count dropped, and every remaining name is now stale.

CREATE does not qualify. It makes a new inode with one name.


STEP 3: THE WALK
================

Before we can mark a directory, two things can be wrong with it.

  a) It may be gone.
     Events describe the filesystem as it was. The directory can be
     deleted before our batch runs.

  b) The index may never have heard of the directories above it.
     A client mounted without an event mask can build a whole subtree
     in silence. So can a burst of events we lost.

Both are fixed by walking up the tree. One walk, two stops:

  First, rise until we land on something that exists and is still a
  directory. That is the deepest thing we can honestly mark.

  Then keep rising while the PARENT has no db.db, marking every level
  we pass. Stop at the first parent the index already has.

Why the parent and not the directory itself?
GUFI creates index directories with plain mkdir, not mkdir -p. It
fails if the parent is missing. So the only question is whether this
directory can be placed. Whether it has a database of its own does
not matter - if it has none, marking it is how it gets one.

Why check for db.db and not just the directory?
A directory can sit in the index with no database. That is what a
run that died halfway leaves behind. If we treated that as "already
indexed", the walk would stop there every time and everything below
it would be stranded for good.


WORKED EXAMPLE
==============

Someone writes to  /mnt/beegfs/d1/d2/d3/report.csv
and d1, d2, d3 were made by a client with events turned off.

  event:    CLOSE_WRITE  path=d1/d2/d3/report.csv  num_links=1

  step 2:   mark d1/d2/d3        (the parent)
            no multi-link file   (only one name)

  step 3:   d3 - parent d2 has no db.db      -> also mark d2
            d2 - parent d1 has no db.db      -> also mark d1
            d1 - parent is the mount, it has db.db -> stop

  output:   2140 d    # d1
            2141 d    # d1/d2
            2142 d    # d1/d2/d3

One event, three marks.

Without the walk we would mark only d3. GUFI's mkdir would fail
because d2 is not in the index, the file would never be indexed,
and the command would still exit 0.


THE MAIN DESIGN CHOICE, FOR REVIEW
==================================

We tell GUFI which directories to re-read.
We do NOT apply the change to the index ourselves.

That means:

  - Order does not matter. A set has no order.
  - Repeats are free. Two marks for one directory are one mark.
  - A late mark still works.
  - A mark we never send leaves that directory stale, not wrong.
    The next event that touches the area repairs it.

The alternative is to replay each operation into the index. That is
faster for mkdir and rename, but it needs every record, in order,
exactly once, plus crash recovery. Miss one and the index is wrong
with nothing to show for it.

(GUFI PR #195 is doing the replay approach for Lustre. The GUFI
maintainer made this same point on it: "the existing incremental
update code does not require knowing what happened in what order -
it just accepts the updated filesystem as is, however it got there.")


KNOWN GAPS
==========

1. We acknowledge the event as soon as it arrives, before GUFI has
   run. A crash in between loses those events silently. A real
   service must not acknowledge until the rescan has finished.

2. If no event for a subtree reaches us at all, nothing marks it.
   It stays stale until some later event touches something inside,
   and then the walk backfills the whole gap at once.

3. This program writes the suspect file and prints the command.
   It does not run GUFI, handle a failed run, or retry a batch.
