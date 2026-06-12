# GUFI Incrementals based on on File Handles

This describes the implementation of file handle based incremental updates for Lustre.

## Motivation

The challenge with performing incremental updates for GUFI is that the GUFI index
mimics the tree-structure of the source filesystem, but changelog records from source filesystems
like Lustre only provide node IDs (i.e., FIDs), not paths.

Given a change record like: "Node with name `child` and id `456` was created under Node with id `123`",
how do you locate node `123` in the index to apply the update there?

Various proposed solutions are either expensive (e.g., require walking the entire source
and index filesystems to resolve node IDs to paths), or vulnerable to race conditions
(e.g., using `lfs fid2path` or analogous operations to resolve a path at some later time when
the node may have been moved to a different location).

Using file handles--the opaque structures returned by the system call `name_to_handle_at(2)`--allows
filesystem nodes to be accessed directly, given their handles, bypassing the need for path resolution.

## File Handle Index

An index needs to be created that maps *source* file handles (e.g., Lustre FIDs) to *index* file handles
in the GUFI tree.
Currently, this is a sqlite database called `filehandles.db` created at the root of the GUFI tree.

`gufi_dir2index` inserts a row into this database each time it creates a directory in the index tree.
The row maps the file handle of the source FS directory to the file handle of the newly created index directory.

## Performing Updates

A program at `src/lustre_changelog_updater.c` consumes Lustre changelogs.
For each changelog record it receives, it records the relevant information and
performs an update to the index tree that mimics the update to the source.

### new or modified files

Updates that do not modify the shape of the tree include file creation, file modification, etc.
These kinds of updates simply trigger the sqlite database at that index node to be recreated.

### mkdir, rmdir

`mkdir` and `rmdir` operations are similar: the changelog record includes the FID
of the parent directory, the name of the affected child, and the FID of the child.

The file handle of the parent directory is looked up in the file handle index.
The parent directory is opened using `open_by_handle_at(2)`, and then the either
`mkdirat(2)` or `unlinkat(2)` is performed.

Then a row is either inserted (for `mkdir`) or deleted (for `rmdir`) from the file handle index.

### rename

`rename` is a little more complicated because there are two parent directories involved.
The changelog record has the FID for each directory, so both the old parent and the
new parent are resolved to index file handles. Both file handles are opened with
`open_by_handle_at(2)`.

Then, `renameat(2)` is performed.

A further complication with `rename` is that rename can fail if the kernel cannot determine
if the rename will not introduce a filesystem loop. The kernel checks this by making sure
that neither parent directory is an ancestor of the other. This check can fail, however,
if the parent directories are in disconnected subtrees of the dentry cache tree.

Normally, the dentry cache tree (or really, forest) does not contain any disconnected subtrees
because files are opened via paths, so any node's parent will already be in memory before
the node itself is opened.

However, opening nodes via their handle instead of their path opens up the possibility that
the node's parent is not cached in the dentry cache. This situation can result in `renameat(2)`
spuriously failing.

In order to resolve this *connectable* file handles must be used.
Connectable file handles ensure that when a node is opened via its handle, the node's
parent is loaded into memory and connected to the child node, and so on recursively
until the node is connected to the main subtree that contains the root.

See https://lwn.net/Articles/993810/ for more details on connectable file handles.
