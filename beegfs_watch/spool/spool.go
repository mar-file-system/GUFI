// Package spool reads the collector's spool files.
//
// A spool file is a stream of beewatch.Event messages, each written with
// protodelim.MarshalTo: the message length as a varint, then the message in
// protobuf binary form. Events keep the meta and sequence IDs Watch gave them,
// so (MetaId, SeqId) identifies an event across replays.
//
// A sealed file (Ext) is complete and never changes. A file still being written
// (Ext + PartialSuffix) can end part way through a record, so only sealed files
// are read.
package spool

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"google.golang.org/protobuf/encoding/protodelim"
)

const (
	// Ext ends the name of a sealed spool file.
	Ext = ".binpb"
	// PartialSuffix follows Ext on a file still being written.
	PartialSuffix = ".partial"
	// SegPrefix starts the name of a spool file: SegPrefix, a zero-padded
	// number, Ext. The collector numbers files from 1 in the order it writes
	// them, and carries on from the highest after a restart.
	SegPrefix = "seg-"
	// segDigits is the zero padding of a segment number. A wider number still
	// parses: readers sort by number, not by name.
	segDigits = 10
)

// Segment is a sealed spool file.
type Segment struct {
	Number uint64
	Path   string
}

// SegmentName returns the sealed name of segment n.
func SegmentName(n uint64) string { return fmt.Sprintf("%s%0*d%s", SegPrefix, segDigits, n, Ext) }

// SegmentNumber returns n for a sealed file named SegPrefix + n + Ext. Files
// still being written, and names from before numbering, return false.
func SegmentNumber(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, SegPrefix)
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, Ext)
	if !ok || digits == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

// Sealed lists the sealed segments in dir numbered above after, lowest first.
// It sorts by number, not by name: the padding is a width, not a limit.
func Sealed(dir string, after uint64) ([]Segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []Segment
	for _, e := range entries {
		if n, ok := SegmentNumber(e.Name()); ok && n > after && e.Type().IsRegular() {
			segs = append(segs, Segment{Number: n, Path: filepath.Join(dir, e.Name())})
		}
	}
	slices.SortFunc(segs, func(a, b Segment) int { return cmp.Compare(a.Number, b.Number) })
	return segs, nil
}

// Reader reads events one at a time from a complete spool file.
type Reader struct {
	r *bufio.Reader
}

// NewReader returns a Reader for r. It buffers r itself; protodelim avoids an
// allocation per record when it is handed a *bufio.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 1<<20)}
}

// Next decodes the next event into ev. It returns io.EOF at the end of the
// file. Any other error means the file is truncated or corrupt from this point.
func (r *Reader) Next(ev *bw.Event) error {
	err := protodelim.UnmarshalFrom(r.r, ev)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("spool record: %w", err)
	}
	return err
}

// SeqGap records seq as seen for meta in last and returns how many events are
// missing just before it. A meta not in last starts at seq. A replay, at or
// below the highest seen, misses nothing: Watch resends what was not acked.
func SeqGap(last map[uint32]uint64, meta uint32, seq uint64) uint64 {
	prev, ok := last[meta]
	if !ok || seq > prev {
		last[meta] = seq
	}
	if !ok || seq <= prev+1 {
		return 0
	}
	return seq - prev - 1
}
