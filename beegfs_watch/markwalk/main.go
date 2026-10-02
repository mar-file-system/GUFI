// markwalk turns the collector's segment files into GUFI suspect files.
//
// It reads sealed segments in number order and turns each batch into one "<inode> d" line per
// directory to rescan, written to <work>/batch-<last segment>/suspects. Only then does the
// state file move past the batch, so a crash repeats a batch and never skips one. Running
// gufi_incremental_update on the file is up to whatever consumes it (DESIGN.md §6).
//
//	go run ./markwalk -spool /local/bt/collector/spool -mount /mnt/beegfs -index /var/lib/gufi
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	bw "github.com/thinkparq/protobuf/go/beewatch"
)

// dirsFor returns the directories an event dirties and, when the inode has other names, its
// entry ID so siblingDirs can find them. A listing change dirties the parent. An inode change
// names the path itself, which markChain keeps if it is a directory (its attributes live in
// its own db.db) and otherwise replaces with the parent.
func dirsFor(ev *bw.V2Event, mount string, trackOpens bool) (dirs []string, entryID string) {
	path := filepath.Join(mount, ev.GetPath()) // "/" for the root makes the mount itself
	parent := filepath.Dir(path)

	switch ev.GetType() {
	case bw.V2Event_INVALID, bw.V2Event_OPEN_BLOCKED, bw.V2Event_INODE_LOCKED:
		// No path, a refused open, and a bare filename: nothing to mark.
		return nil, ""

	case bw.V2Event_MKDIR:
		// The new directory needs its own line, or GUFI creates it without a db.db.
		return []string{parent, path}, ""

	case bw.V2Event_RENAME:
		// Fired once, on the source. Without the common ancestor GUFI sees a delete plus a
		// create and throws the moved databases away instead of moving them.
		target := filepath.Join(mount, ev.GetTargetPath())
		dst := filepath.Dir(target)
		return []string{commonAncestor(parent, dst), parent, dst, target}, ""

	case bw.V2Event_HARDLINK:
		// nlink is stored per name, so the other name's directory is stale too.
		return []string{parent, filepath.Dir(filepath.Join(mount, ev.GetTargetPath()))}, linkedEntryID(ev)

	case bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if !trackOpens { // only atime, and very many events
			return nil, ""
		}
		return []string{parent}, linkedEntryID(ev)

	case bw.V2Event_SETATTR, bw.V2Event_STRIPE_PATTERN_CHANGED:
		return []string{path}, linkedEntryID(ev) // can be a directory's own inode

	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED:
		// Only files get these, so name the parent directly: naming the file costs a stat of
		// every written file, millions of gone names for a tree written and removed.
		return []string{parent}, linkedEntryID(ev)

	default:
		// CREATE, MKNOD, SYMLINK, RMDIR, UNLINK, and any type added later: the listing changed.
		return []string{parent}, linkedEntryID(ev)
	}
}

// linkedEntryID returns the event's entry ID when it changed an inode that has other names,
// whose directories then report a stale size, times or link count. UNLINK counts the names
// left after it (3 names, rm -> 2); every other type counts its own name too.
func linkedEntryID(ev *bw.V2Event) string {
	id := ev.GetEntryId()
	if !entryIDPattern.MatchString(id) {
		return ""
	}
	switch ev.GetType() {
	case bw.V2Event_UNLINK:
		if ev.GetNumLinks() >= 1 {
			return id
		}
	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_SETATTR,
		bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED,
		bw.V2Event_STRIPE_PATTERN_CHANGED, bw.V2Event_HARDLINK,
		bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if ev.GetNumLinks() > 1 {
			return id
		}
	}
	return ""
}

// entryIDPattern matches a BeeGFS entry ID: three 32-bit hex fields (StorageTk::generateFileID)
// or a reserved name. IDs go into SQL text on a command line, so they are checked, not escaped.
var entryIDPattern = regexp.MustCompile(`^(?:[0-9A-Fa-f]{1,8}-[0-9A-Fa-f]{1,8}-[0-9A-Fa-f]{1,8}|root|disposal|mdisposal)$`)

// idsPerQuery keeps each query's SQL under 100 KiB: Linux refuses a single argument of
// 128 KiB (MAX_ARG_STRLEN) with E2BIG, and a quoted ID takes at most 29 bytes.
const idsPerQuery = (100 << 10) / 29

// siblingDirs asks the index which directories list these entry IDs, through the
// beegfs_entries table the BeeGFS plugin fills in. Every query walks the whole index, so
// it asks for as many IDs at once as fit; a failed query loses only its own IDs.
func siblingDirs(entryIDs []string, mount, indexRoot, queryBin string) []string {
	if queryBin == "" {
		return nil
	}
	var dirs []string
	seen := map[string]bool{}
	for chunk := range slices.Chunk(entryIDs, idsPerQuery) {
		sql := "SELECT path() FROM beegfs_entries WHERE entry_id IN ('" + strings.Join(chunk, "','") + "');"
		out, err := exec.Command(queryBin, "-d", "\n", "-E", sql, indexRoot).Output()
		if err != nil {
			slog.Warn("sibling lookup failed; other names of these multi-name files keep a stale size",
				"entry_ids", len(chunk), "error", err)
			continue
		}
		for _, line := range strings.Split(string(out), "\n") {
			if line = strings.TrimSpace(line); line == "" {
				continue
			}
			rel, err := filepath.Rel(indexRoot, line)
			if err != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			if src := filepath.Join(mount, rel); !seen[src] {
				seen[src] = true
				dirs = append(dirs, src)
			}
		}
	}
	return dirs
}

// statCache remembers, for one batch, whether a path is a directory and its inode, so paths
// many marks share cost one metadata round trip. It drops the least recently used answer
// beyond its limit; forgetting only means asking again.
type statCache struct {
	lru   *simplelru.LRU[string, dirStat]
	stats int // os.Stat calls made
}

type dirStat struct {
	inode uint64
	isDir bool
}

func newStatCache(limit int) *statCache {
	lru, err := simplelru.NewLRU[string, dirStat](max(limit, 1), nil)
	if err != nil {
		panic(err) // only for a size below 1
	}
	return &statCache{lru: lru}
}

// stat reports whether path is a directory now, and its inode. The type check matters: a
// directory replaced by a file must not reach the suspect file as a directory.
func (c *statCache) stat(path string) (inode uint64, isDir bool) {
	if s, ok := c.lru.Get(path); ok {
		return s.inode, s.isDir
	}
	c.stats++
	var s dirStat
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		s = dirStat{info.Sys().(*syscall.Stat_t).Ino, true}
	}
	c.lru.Add(path, s)
	return s.inode, s.isDir
}

// markChain returns the directories to mark for dir, shallowest first. It rises to the
// nearest path that is still a directory, then keeps rising while the parent has no db.db:
// GUFI creates index directories with a plain mkdir, so a parent missing from the index must
// be marked too. It checks for db.db rather than the directory because a GUFI run that died
// half way leaves directories without one.
func markChain(dir, mount, indexRoot string, stat func(string) (uint64, bool)) []string {
	cur := dir
	for {
		if _, ok := stat(cur); ok {
			break
		}
		if cur = filepath.Dir(cur); !within(cur, mount) {
			return nil
		}
	}
	chain := []string{cur}
	for cur != mount {
		parent := filepath.Dir(cur)
		rel, _ := filepath.Rel(mount, parent)
		if info, err := os.Stat(filepath.Join(indexRoot, rel, "db.db")); err == nil && info.Mode().IsRegular() {
			break
		}
		chain = append([]string{parent}, chain...)
		cur = parent
	}
	return chain
}

// writeSuspects writes one "<inode> d" line per directory to mark for dirs, deduplicated by
// inode, and returns how many lines and how many stats that took. With no lines it writes
// nothing.
func writeSuspects(dirs []string, mount, indexRoot, out string, cacheSize int) (marks, stats int, err error) {
	cache := newStatCache(cacheSize)
	inodes := map[uint64]struct{}{}
	for _, d := range dirs {
		for _, c := range markChain(d, mount, indexRoot, cache.stat) {
			if ino, ok := cache.stat(c); ok {
				inodes[ino] = struct{}{}
			}
		}
	}
	if len(inodes) == 0 {
		return 0, cache.stats, nil
	}
	var b strings.Builder
	for ino := range inodes {
		fmt.Fprintf(&b, "%d d\n", ino) // unsigned: GUFI matches inodes as text
	}
	return len(inodes), cache.stats, writeFileDurable(out, []byte(b.String()))
}

// commonAncestor returns the deepest directory that holds both a and b.
func commonAncestor(a, b string) string {
	for !within(b, a) && filepath.Dir(a) != a {
		a = filepath.Dir(a)
	}
	return a
}

// within reports whether path is root or below it.
func within(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

func main() {
	spoolDir := flag.String("spool", "/var/lib/index-sync/spool", "the collector's segment directory")
	statePath := flag.String("state", "/var/lib/index-sync/markwalk.json", "how far markwalk got")
	work := flag.String("work", "/var/lib/index-sync/markwalk", "where each batch's suspect file goes")
	mount := flag.String("mount", "/mnt/beegfs", "the BeeGFS mount, so paths can be made absolute")
	index := flag.String("index", "/var/lib/gufi", "GUFI index root")
	query := flag.String("query", "gufi_query", "gufi_query binary, used to find the other names of a multi-link inode")
	every := flag.Duration("every", time.Minute, "how long a batch collects before it is made")
	poll := flag.Duration("poll", 5*time.Second, "how often to look for new segments when there are none")
	maxSegs := flag.Int("max-segments", 100, "most segments in one batch")
	cacheSize := flag.Int("stat-cache", 200_000,
		"most stat answers a batch keeps, least recently used dropped first (~150 bytes + path each)")
	opens := flag.Bool("track-opens", false, "include open events, for atime")
	once := flag.Bool("once", false, "process the segments there now and exit")
	demo := flag.Bool("demo", false, "run the walk against a temp tree and exit")
	flag.Parse()

	if *demo {
		runDemo()
		return
	}
	if *maxSegs < 1 || *cacheSize < 1 || *every <= 0 || *poll <= 0 {
		slog.Error("max-segments and stat-cache must be at least 1, every and poll above 0")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := &reader{
		log: slog.Default(), spool: *spoolDir, statePath: *statePath, work: *work,
		mount: filepath.Clean(*mount), index: filepath.Clean(*index), query: *query,
		trackOpens: *opens, maxSegments: *maxSegs, statCacheSize: *cacheSize,
	}
	if err := r.run(ctx, *every, *poll, *once); err != nil {
		slog.Error("markwalk stopped", "error", err)
		os.Exit(1)
	}
}
