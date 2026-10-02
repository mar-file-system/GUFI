package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/google/renameio/v2"
	"github.com/thinkparq/beegfs-index/beegfs_watch/spool"
	bw "github.com/thinkparq/protobuf/go/beewatch"
)

// state is how far markwalk got.
type state struct {
	// Segment is the last segment whose suspect file is written; the next batch starts after it.
	Segment uint64 `json:"segment"`
	// Seqs is each meta's highest sequence ID seen, so holes are found across batches and restarts.
	Seqs map[uint32]uint64 `json:"seqs"`
}

func readState(path string) (state, error) {
	st := state{Seqs: map[uint32]uint64{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, fmt.Errorf("read state: %w", err)
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("%s: %w", path, err)
	}
	if st.Seqs == nil {
		st.Seqs = map[uint32]uint64{}
	}
	return st, nil
}

// writeFileDurable replaces path with data so that after a crash it holds either the old or
// the new content. renameio writes, fsyncs and renames a temp file; the directory is then
// fsynced too, so the rename itself survives a crash before the state file moves past it.
func writeFileDurable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := renameio.WriteFile(path, data, 0o644, renameio.WithTempDir(dir)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// reader turns segments into suspect files.
type reader struct {
	log           *slog.Logger
	spool         string // the collector's segment directory
	statePath     string
	work          string // one directory per batch: batch-<last segment>/suspects
	mount         string
	index         string
	query         string // gufi_query, for the sibling lookup
	trackOpens    bool
	maxSegments   int
	statCacheSize int
}

// run makes batches until ctx is done, waiting every between batches and poll when there is
// nothing new. With once it makes batches of what is there now and returns.
func (r *reader) run(ctx context.Context, every, poll time.Duration, once bool) error {
	st, err := readState(r.statePath)
	if err != nil {
		return err
	}
	r.log.Info("markwalk started", "spool", r.spool, "after_segment", st.Segment, "state", r.statePath)
	for {
		segs, err := spool.Sealed(r.spool, st.Segment)
		if err != nil {
			return err
		}
		if len(segs) > r.maxSegments {
			segs = segs[:r.maxSegments]
		}
		wait := poll
		if len(segs) > 0 {
			if err := r.process(&st, segs); err != nil {
				return err
			}
			wait = every
		} else if once {
			return nil
		}
		if once {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// process writes the suspect file for segs, then moves st past them. A crash in between
// repeats the batch, which writes the same file again.
func (r *reader) process(st *state, segs []spool.Segment) error {
	first, last := segs[0].Number, segs[len(segs)-1].Number
	seqs := maps.Clone(st.Seqs)
	dirs := map[string]struct{}{}
	links := map[string]struct{}{} // entry IDs of multi-name files, see siblingDirs
	var events, v1, missing uint64

	for _, s := range segs {
		err := readSegment(s.Path, func(ev *bw.Event) {
			events++
			if n := spool.SeqGap(seqs, ev.GetMetaId(), ev.GetSeqId()); n > 0 {
				missing += n
				r.log.Warn("missing events: the spool has a hole, those directories may stay stale",
					"meta", ev.GetMetaId(), "from", ev.GetSeqId()-n, "to", ev.GetSeqId()-1,
					"missing", n, "segment", s.Number)
			}
			v2 := ev.GetV2()
			if v2 == nil {
				v1++ // BeeGFS 7 format, not used
				return
			}
			ds, id := dirsFor(v2, r.mount, r.trackOpens)
			for _, d := range ds {
				dirs[d] = struct{}{}
			}
			if id != "" {
				links[id] = struct{}{}
			}
		})
		if err != nil {
			return err // a damaged segment stops markwalk rather than skip what it held
		}
	}

	marked := append(slices.Collect(maps.Keys(dirs)),
		siblingDirs(slices.Collect(maps.Keys(links)), r.mount, r.index, r.query)...)
	out := filepath.Join(r.work, fmt.Sprintf("batch-%010d", last), "suspects")
	marks, stats, err := writeSuspects(marked, r.mount, r.index, out, r.statCacheSize)
	if err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}

	st.Segment, st.Seqs = last, seqs
	data, err := json.Marshal(st)
	if err == nil {
		err = writeFileDurable(r.statePath, append(data, '\n'))
	}
	if err != nil {
		return fmt.Errorf("write %s: %w", r.statePath, err)
	}

	if marks == 0 {
		out = "none: nothing left to mark"
	}
	r.log.Info("batch done", "first_segment", first, "last_segment", last, "segments", len(segs),
		"events", events, "directories", len(dirs), "multi_name_inodes", len(links), "marks", marks,
		"stats", stats,
		"v1_events_skipped", v1, "missing_events", missing, "suspects", out)
	return nil
}

// readSegment calls fn for every event in a segment. fn must not keep the event, which is reused.
func readSegment(path string, fn func(*bw.Event)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sr := spool.NewReader(f)
	var ev bw.Event
	for {
		err := sr.Next(&ev)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		fn(&ev)
	}
}
