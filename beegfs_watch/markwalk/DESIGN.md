# markwalk redesign — consume the collector's segment files

Related: `../DESIGN.md` (the collector that produces the segment files), `./FLOW.md` (the
event→suspect-file logic), ThinkParQ/beegfs-index issue #5.

---

## 1. What changes, in one sentence

Today `markwalk` **is a gRPC subscriber**: Watch dials into it and it processes events live
(`ReceiveEvents`). In the new architecture the **collector** owns the gRPC endpoint and persists
events to **numbered segment files**; `markwalk` becomes a **reader of those files** that runs the
beegfs-index `gufi_incremental_update` itself and only then moves its place forward. The
event→directory logic (Parts 1–3) is kept, with the fixes in §5; the input source, progress and the
GUFI run change.

```
BEFORE:  Watch ──gRPC──► markwalk (subscriber) ──► suspect file ──► (operator runs GUFI)

AFTER:   Watch ──gRPC──► collector ──► seg-<N>.binpb (durable, acked)
                                           │
                                           ▼
                          markwalk (reader) ──► suspect file ──► gufi_incremental_update ──► index
                                           │
                                           └─ state: last segment done; done segments deleted
```

Why this is better: the collector acks Watch only once a segment is durable (fsync, link, directory
fsync; `../DESIGN.md` §7, §9), so Watch's buffer is freed at disk speed, whatever markwalk is doing.
markwalk can be slow, crash or restart without any interaction with Watch; the segments on disk are
the queue between them.

---

## 2. Input: the segment files

Produced by the collector; `../DESIGN.md` §6 is the source of truth. In short:

```
spool dir:  seg-0000000001.binpb, seg-0000000002.binpb, …    sealed, immutable
            seg-0000000003.binpb.partial                     being written: never read
record:     [uvarint len][bw.Event]                          protodelim; meta_id, seq_id inside
```

- The number is zero-padded to ten digits, strictly increasing, and continues after a collector
  restart, so **a reader's place is one number**. Parse it (`seg-(\d+)\.binpb`) and sort by value,
  not by name.
- Read records with the `spool` package: `spool.NewReader(f)` and `Next()` until `io.EOF`. It is
  the format's reference reader; markwalk does not parse bytes itself.
- The `*bw.Event` gives `GetMetaId()`, `GetSeqId()` and `GetV2()`; on the V2 message `GetType()`,
  `GetPath()`, `GetTargetPath()`, `GetNumLinks()`, `GetEntryId()`, `GetParentEntryId()`,
  `GetTargetParentId()`, `GetTimestamp()`, `GetMsgUserId()`. V1 events (`GetV2() == nil`) are
  counted and logged, not used.

---

## 3. New markwalk structure

Delete the gRPC server; add the spool reader and the GUFI run.

| Concern | Current (`main.go`) | Change |
|---|---|---|
| Transport | `ReceiveEvents` gRPC server, `net.Listen`, `grpc.NewServer` | **Remove.** Replace with the reader loop below. |
| Event → dirs | `dirsFor`, `linkedEntryID` | **Keep**, with §5's fixes (done: INVALID, inode changes, UNLINK). |
| Batch set | `collector.add`, `pending`/`links` maps | **Keep**, fed by the reader. Rename the type (`batch`). |
| Walk | `markChain`, `statCache` | **Keep unchanged.** |
| Sibling lookup | `siblingDirs` | **Keep**; needs §6.3 to keep working after a rescan. |
| Suspect file | `writeSuspects` | **Keep** (`<inode> d` lines, §6.1). |
| GUFI | printed command | **Run it** (§6.2), per batch, in a work directory removed afterwards. |
| Progress | acks to Watch | **Replace** with the last-segment state file (§4). |
| Retention | none | **Delete** each segment once its batch's rescan succeeded (§4). |

Reader loop (shape):

```
done := readState(statePath)                        // last segment number whose rescan succeeded
deleteSegments(spool, <= done)                      // a crash between state write and delete
for {
    segs := sealedSegments(spool, > done)           // by number; never *.partial
    if len(segs) == 0 { sleep(poll); continue }

    b := newBatch()
    for _, seg := range segs {                      // cut at -every, or -max-segments
        r := spool.NewReader(open(seg))
        for ev, err := r.Next(); err == nil; ev, err = r.Next() {
            noteSeq(ev.GetMetaId(), ev.GetSeqId())  // gaps: log and count only (§7)
            if v2 := ev.GetV2(); v2 != nil {
                b.add(dirsFor(v2, mount, trackOpens))
            }
        }
        b.last = seg.number
        if b.full() { break }
    }

    if b.empty() || runGUFI(b) == nil {             // §6.2: suspects, run, clean up
        write state(statePath, b.last)              // renameio + fsync of the directory
        deleteSegments(spool, <= b.last)
        done = b.last
    }                                               // on failure: keep everything, retry later
}
```

---

## 4. Consumption, progress and retention

- **Order:** ascending segment number; records in file order. Suspect-file correctness does not
  depend on it (a set has no order, §8), but it keeps progress a single number.
- **Only sealed segments.** Never open a `*.partial`.
- **Progress** (`-state`): the highest segment number whose batch's rescan succeeded, written
  atomically (temp file, fsync, rename). On restart, resume after it.
- **Reprocess-safe.** A crash after the rescan but before the state write reprocesses those
  segments: the same directories are marked again, and `gufi_incremental_update` "accepts the
  updated filesystem as is, however it got there" (issue #5).
- **Duplicates** (a collector crash between sealing and Watch receiving the ack replays events that
  are already sealed) need no handling: the batch is a set of directories, and a directory marked
  twice is marked once. No per-meta dedup state is kept; a "skip seq ≤ last seen" rule would drop
  everything from a meta whose event queue was recreated and restarted at 0.
- **Retention is markwalk's job.** The collector never deletes. markwalk deletes a segment once its
  batch's rescan succeeded and the state file says so; at ~78 B per event, 1M events/s is ~280 GB an
  hour, so this is what keeps the spool bounded. `-keep-segments` turns it off for test clusters,
  where the test tools read the same spool.

---

## 5. Event-type handling

Against the full `beewatch.V2Event_Type` enum (protobuf `v0.8.5-…`, values 0–19). **Guiding rule:**
GUFI rescans *directories*. A listing change names the parent. An inode change names the path
itself, and the walk (`markChain`) keeps it if it is a directory — whose own mode, owner and times
live in its own `db.db` — and otherwise rises to the parent, where a file's are recorded.

| # | Type | Dirties | Siblings (`linkedEntryID`) | Notes |
|---|------|---------|----------|-------|
| 0 | `INVALID` | nothing | — | no path; joining `""` would name the mount's parent |
| 1 | `FLUSH` | the path: itself if a dir, else its parent | `num_links > 1` | inode changed |
| 2 | `TRUNCATE` | as FLUSH | `num_links > 1` | |
| 3 | `SETATTR` | as FLUSH | `num_links > 1` | `chmod` of a dir marks the dir; of the root, the mount |
| 4 | `CLOSE_WRITE` | as FLUSH | `num_links > 1` | |
| 5 | `CREATE` | parent | — | listing only |
| 6 | `MKDIR` | parent **and** the new dir | — | the new dir needs its own db.db |
| 7 | `MKNOD` | parent | — | |
| 8 | `SYMLINK` | parent | — | |
| 9 | `RMDIR` | parent | — | the walk handles "now missing" |
| 10 | `UNLINK` | parent | `num_links ≥ 1` | the other names' link count dropped |
| 11 | `HARDLINK` | both parents (new link's and target's) | `num_links > 1` | path = new link, target = existing |
| 12 | `RENAME` | common ancestor + both parents | — | path = old, target = new; one event |
| 13–15 | `OPEN_*` | as FLUSH, only with `-track-opens` | `num_links > 1` | atime only; high volume |
| 16 | `LAST_WRITER_CLOSED` | as FLUSH | `num_links > 1` | |
| 17 | `OPEN_BLOCKED` | nothing | — | the open was refused |
| 18 | `STRIPE_PATTERN_CHANGED` | as FLUSH | `num_links > 1` | |
| 19 | `INODE_LOCKED` | nothing | — | path is a bare filename |
| — | a type added later | parent | as today | a rescan too many, never one too few |

Measured on a v8 cluster (2026-09-29), which the rows above rely on:
- `chmod` on the file system root gives `SETATTR path="/"`; the old parent rule named `/mnt`,
  outside the file system. On a directory, `path` is the directory itself.
- `num_links` on UNLINK is the number of names **left**: three names, `rm` → 2, `rm` → 1, and the
  last `rm` reports none. On HARDLINK and writes it includes the event's own name. So UNLINK needs
  the sibling query at `≥ 1`, everything else at `> 1`.

These are implemented in `main.go` (`dirsFor`, `linkedEntryID`) with `main_test.go` covering each
row. `entryIDPattern` (BeeGFS's three 32-bit hex fields, or a reserved name) still gates every entry ID before it reaches `gufi_query`, where it is
interpolated into SQL text.

---

## 6. Running the beegfs-index incremental update

markwalk stops at the suspect file. Running the update is the job of whatever consumes
`<work>/batch-*/suspects`, in batch order. This section describes that consumer. An earlier version
that ran the update inside markwalk is kept on the local branch `markwalk-incremental-update`.

### 6.1 Suspect file

- `--suspect-method 1` + `--suspect-file`. Mode 1 reads only `d` lines, in the old fork and in
  upstream alike (both checked with a test tree, 2026-10-01). So `writeSuspects`
  writes `<inode> d`, deduplicated by inode, and nothing else.
- GUFI matches inodes as strings, so the number must be printed unsigned (`writeSuspects` does).
  BeeGFS inodes are 64-bit hashes, half of them ≥ 2^63; the package must include `fc9e33e8`
  (unsigned inodes in the snapshot; `e2f5d4a7` on the upstream rebase), or those directories are silently not updated.
- Mode 1 because Watch says exactly which directories changed. Mode 3 (`--suspect-time`) relies on
  directory mtime/ctime and misses content changes that do not touch them (issue #5).

### 6.2 The command, per batch

beegfs-index now carries current upstream GUFI (branch `sunder/rebase-upstream-gufi`). Its
`gufi_incremental_update` takes **three** positional arguments, with no snapshot database:

```
gufi_incremental_update -n <threads> --suspect-method 1 --suspect-file <run>/suspects \
    --plugin beegfs_index_ops:/opt/beegfs/lib/libbeegfs_indexing.so \
    <index> <mount> <run>/parking
```

The plugin is the same `beegfs_index_ops` that `gufi_dir2index` uses, so every directory the update
rebuilds keeps its BeeGFS tables.

What the update does by itself, with no help from the suspect file:
- it walks the whole tree and diffs it against the index by inode, so it finds every added, removed
  and moved directory;
- it rebuilds the db.db of every changed directory it has no database for, which covers new
  directories and directories made and then moved (upstream `72285334`). markwalk therefore no
  longer walks moved trees for unindexed subdirectories (the old `newDirsUnder`).

What still needs the suspect file: a directory whose **contents** changed while the directory itself
stayed where it was. That is markwalk's job.

- `<run>` is a fresh directory under `-work` for this batch; the parking lot holds moved
  directories and new db.dbs. After a successful run markwalk removes `<run>` entirely. After a
  failed run it keeps the newest few for diagnosis and removes older ones.
- Run it with `<run>` as the working directory. It makes a temporary database in its working
  directory and exits 1 if it can't (`Could not create temporary db …: Permission denied` from `/`).
- Only exit 0 moves progress forward (§4). Exit 0 is necessary, not sufficient: the inode bug fixed
  in `fc9e33e8` failed silently with exit 0. A cheap check after the run — every suspect directory
  has a `db.db` in the index — catches that class.
- A batch with no marks runs nothing and still moves progress forward.

Two upstream bugs in the rebuild of directories with no database are fixed on the rebase branch
(`d3954a81`, `aa75bdb8`): the rebuild used the shared, not the worker's own, plugin context
(a crash with a plugin and more than one thread), and it stat'ed a path relative to the tree's
parent (wrong unless run from there).

### 6.3 Superseded: the `beegfs_incremental_ops` plugin

The old fork's `gufi_incremental_update` accepted only `PLUGIN_QUERY` plugins, so the BeeGFS
indexing plugin (`PLUGIN_INDEX`) was refused, and a second entry point, `beegfs_incremental_ops`,
was added with the same hooks and the query type. The old fork also took four arguments
(`<index> <mount> <snapshot> <parking>`) and left new directories bare unless the suspect file
named them.

Upstream checks `PLUGIN_INDEX` again and indexes through the same code as `gufi_dir2index`
(`src/index.c`), so `beegfs_index_ops` works directly and `beegfs_incremental_ops` is gone. The
plugins were ported to the current plugin API, and `fc9e33e8` (unsigned inodes in the snapshot) is
carried forward as `e2f5d4a7`.

### 6.4 Cost of a run

Mode 1 still walks the whole source tree to find the suspect inodes and diffs it against the
index, so a run costs time proportional to the file system, not to the batch. Run it
minutes apart, not seconds: `-every` sets how long a batch collects before its run. Events keep
arriving in segments meanwhile; nothing waits.

---

## 7. Gaps in sequence numbers

- A per-meta jump in `seq_id` means events never reached the spool: Watch's buffer overflowed, or a
  first connection skipped to the end of an unseen meta's buffer (see the collector's checkpoint
  notes).
- Effect on the index (FLOW.md gap #2): that subtree stays stale until a later event touches it,
  then the walk backfills it. Log the gap and count it (per meta, events missing); no special
  handling is needed for correctness. A large gap is the signal to schedule a full rescan.
- `metaId` does not change path interpretation (paths are global, mount-relative). Mirrored metas
  (`Event.GetMetaMirror()`) are an open question (§12); the test cluster has none.

---

## 8. Batching, ordering, and why order-independence still holds

Unchanged from FLOW.md:

- markwalk emits **which directories to re-read**, not a replay of operations.
- So order does not matter, repeats are free, a late mark still works, and a missed mark leaves a
  directory stale (not wrong) until the next event repairs it.
- The three cases that look order-sensitive are handled structurally:
  - RENAME → both parents + common ancestor, so GUFI moves instead of delete+create;
  - MKDIR → the new directory too, so it gets a db.db;
  - a directory replaced by a file → `statCache.stat`'s type check refuses a non-directory inode.

So a batch may span any number of segments and metas: many segments → one suspect file → one run.

---

## 9. Flags / CLI

Status: **done** is in `main.go`/`reader.go`; **consumer** belongs to whatever runs the update (§6), not markwalk.

| flag | status | purpose |
|---|---|---|
| `-listen`, `-out` | removed | no longer a gRPC server; the suspect file lives in the batch directory |
| `-spool` | done | the collector's segment directory |
| `-state` | done | the last segment whose batch is done, and each meta's highest sequence ID |
| `-work` | done | one directory per batch, `batch-<last segment>/suspects`; the consumer removes it once applied |
| `-every` | done, new meaning | how long a batch collects before it is made (default 1m; minutes in production) |
| `-poll` | done | how often to look again when there are no new segments (default 5s) |
| `-max-segments` | done | most segments in one batch (default 100) |
| `-once` | done | process the segments there now and exit (tests, cron) |
| `-mount`,`-index`,`-query`,`-track-opens`,`-demo` | kept | unchanged |
| `-gufi`, `-threads` | consumer | `gufi_incremental_update` binary and its `-n` |
| `-plugin` | consumer | `beegfs_index_ops:<lib>` (§6.2) |
| `-keep-segments` | consumer | do not delete processed segments (test clusters) |
| `-dry-run` | consumer | write the suspect file and print the command; never run it, never move progress |

`-demo` (`demo.go`) prints what `markChain` marks for five awkward trees.

---

## 10. Performance / resource notes

- Per event: one `protodelim` decode, a type switch and map inserts. Segments are durable, so lag
  is harmless: markwalk only has to drain the spool over time, not keep up with peaks.
- Reuse one `spool.Reader` buffer; the batch maps are O(distinct directories), not O(events).
- The expensive steps are, in order: the GUFI run (§6.4, whole tree), the walk (a stat per level)
  and the sibling query. The last two are already once per batch; keep them there.
- The sibling query passes its SQL to `gufi_query` as one argument, and Linux refuses any argument
  of 128 KiB or more (E2BIG). An entry ID is at most 26 characters, so `siblingDirs` asks for
  `idsPerQuery` (3,531) IDs per query, under 100 KiB. A batch of many multi-name files costs one
  index walk per 3,531 IDs instead of losing all of them. A failed query loses only its own IDs,
  with a warning.
  Memory is O(distinct multi-name entry IDs) per batch, at worst about 1 GB for 10M.

---

## 11. Migration steps

1. Done: the collector names segments `seg-<N>.binpb` and continues numbering after a restart;
   `dirsFor`/`linkedEntryID` handle INVALID, inode changes on directories and the root, and UNLINK.
2. Done: the reader (`reader.go`). The `spool` package gained `SegmentNumber` and `Sealed`, the
   numbered listing every reader shares. A batch of up to `-max-segments` sealed segments becomes
   `<work>/batch-<last>/suspects`, written atomically, and only then is the state file (also atomic)
   moved past it. A damaged segment stops markwalk with its state unchanged.
3. Done: `ReceiveEvents`, `net.Listen`, `grpc.NewServer`, `-listen` and `-out` are gone.
4. Out of scope for markwalk: the run (§6.2) lives in the consumer of the suspect files. markwalk's
   state moves after the suspect file is written, so the consumer must apply batches in order and
   track its own progress.
5. Retention: segments ≤ the state can be deleted once the consumer has applied their batch; the
   consumer owns this, not markwalk.
6. Gap logging (§7): done, per meta, carried across restarts in the state file. Counters are later.
7. beegfs-index: done, rebased onto upstream GUFI (§6.2, §6.3); the consumer passes `beegfs_index_ops`
   with `--plugin`.
8. Update `README.md` and `FLOW.md` (the picture becomes `collector → segments → markwalk → GUFI`;
   gap #1 is closed by §4).
9. Tests: done for the reader (`reader_test.go`: batches, many segments to one batch, `.partial`
   left alone, holes across runs, a damaged segment, an empty batch). Still to add: a golden
   segment covering every enum value, and a crash test for the consumer.

---

## 12. Open questions for the team

- **Plugin in the incremental update:** decided, a `beegfs_incremental` plugin (§6.3). Upstream
  GUFI has since fixed the check; the workaround goes in the update patch. Still open: when that
  patch happens, and sending `fc9e33e8` (unsigned inodes) upstream, which is needed either way.
- **Mirrored metas:** does Watch report an event once per buddy group, or from both buddies?
  Harmless for correctness (directories are a set), but it doubles the work.
- **Suspect stat:** mode 1 ignores `--suspect-time` unless `--suspect-stat` is set. Trust Watch
  entirely (current plan), or also confirm suspects by stat?
- **GUFI version:** pin the beegfs-index build (8.4.1 + `fc9e33e8`); issue #5 shows mode-1
  behaviour changed across GUFI commits.
