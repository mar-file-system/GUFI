// Command collector receives events from one or more BeeGFS Watch instances and
// writes them to spool files for the processor to pick up.
//
// Events are appended, exactly as Watch sent them, to seg-<N>.binpb.partial:
// protobuf binary, each record prefixed with its length (protodelim). The spool
// package reads them. When a file holds enough
// events, or has been open long enough, it is handed to a sealer, which fsyncs
// it, gives it its final seg-<N>.binpb name, fsyncs the directory, and only then
// acks its events back to Watch. A sealed file is therefore complete and on
// disk, and is the processor's to take.
//
// N counts up from 1, zero-padded, and continues from the highest sealed file
// after a restart, so name order is number order is arrival order. A reader
// can keep its place as a single number.
//
// Nothing here can slow down the file system: the metadata service and Watch
// never wait for us. What falling behind costs is Watch's buffer, which drops
// the oldest events once it is full. So the receive path never waits on disk,
// and the sealer only gets in its way when fsync is several files behind.
//
// Watch replays anything we never ack, so a crash costs a replay and nothing
// else. What Watch no longer holds cannot come back: its buffer overflowed, or
// the metadata service discarded events while Watch was away. Sequence IDs are
// per meta and count up by one, so the receive loop logs every jump as
// "missing events", from the first event after a restart on (it starts from the
// checkpoint).
// Any .partial left behind is incomplete and is deleted at startup. A
// disk error stops the collector instead of guessing: nothing unsealed is acked,
// and the replay after a restart fills the hole.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	"github.com/thinkparq/beegfs-index/beegfs_watch/spool"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// seqTracker finds holes in each meta's sequence IDs. Only the receive loop
// uses it.
type seqTracker struct {
	last    map[uint32]uint64 // highest sequence ID seen, per meta
	missing map[uint32]uint64 // events skipped so far, per meta
}

// newSeqTracker starts from the checkpoint, where Watch resumes, so a hole that
// opened while the collector was down is found on the first event after it.
// The checkpoint trails the spool by up to -checkpoint-every, so the first hole
// after a crash can include a few events that were sealed just before it.
//
// A meta that is not in the checkpoint, or only as the SDK's "seek to end"
// placeholder (MaxUint64), starts at its first event.
func newSeqTracker(checkpoint map[uint32]uint64) *seqTracker {
	t := &seqTracker{last: make(map[uint32]uint64, len(checkpoint)), missing: map[uint32]uint64{}}
	for meta, seq := range checkpoint {
		if seq != math.MaxUint64 {
			t.last[meta] = seq
		}
	}
	return t
}

// see records seq for meta and returns how many events were skipped just
// before it. A replay (seq at or below the highest seen) skips nothing.
func (t *seqTracker) see(meta uint32, seq uint64) uint64 {
	n := spool.SeqGap(t.last, meta, seq)
	if n > 0 {
		t.missing[meta] += n
	}
	return n
}

// file is one spool file and the acks owed for what is in it.
//
// acks holds one entry per event (16 B), because the SDK takes acks one event at
// a time and keeps its own 16 B per unacked event as well. Up to seal-queue + 2
// files are in flight (the open one, the queue, the one being sealed), so
// roll-events bounds that memory, and also how much of Watch's buffer sits
// unacked. Once the SDK can ack a whole prefix (DESIGN.md D8, AckUpTo), this
// becomes each meta's highest SeqId per file.
type file struct {
	f    *os.File
	name string
	acks []subscriber.Ack
}

type collector struct {
	log       *zap.Logger
	dir       string
	maxEvents int

	events chan *bw.Event
	acks   chan subscriber.Ack
	sealq  chan *file // written, waiting for fsync; the sealer is its only reader

	// Owned by the receive loop.
	cur  *file
	w    *bufio.Writer // reset onto each new file
	last uint64        // number of the newest file, sealed or open
	seqs *seqTracker
	enc  []byte                      // the last event's encoding; reused, it grows to the largest event seen
	size [binary.MaxVarintLen64]byte // its length prefix; here, not on the stack, which Write would make escape
}

// prepare creates the spool directory, removes any file left mid-write, and
// continues numbering after the highest sealed file.
func (c *collector) prepare() error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	old, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	for _, e := range old {
		if n, ok := spool.SegmentNumber(e.Name()); ok {
			c.last = max(c.last, n)
			continue
		}
		if !strings.HasSuffix(e.Name(), spool.PartialSuffix) {
			continue
		}
		// Its events were never acked, so Watch replays them into a new file.
		// Its number is free again.
		if err := os.Remove(filepath.Join(c.dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (c *collector) add(ev *bw.Event) error {
	if n := c.seqs.see(ev.GetMetaId(), ev.GetSeqId()); n > 0 {
		meta, seq := ev.GetMetaId(), ev.GetSeqId()
		c.log.Warn("missing events: Watch no longer had them",
			zap.Uint32("meta", meta), zap.Uint64("from", seq-n), zap.Uint64("to", seq-1),
			zap.Uint64("missing", n), zap.Uint64("missing_total", c.seqs.missing[meta]))
	}
	if c.cur == nil {
		// O_EXCL here and the sealer's link, which refuses to replace a file,
		// make a reused number an error rather than a lost file.
		c.last++
		name := spool.SegmentName(c.last)
		f, err := os.OpenFile(filepath.Join(c.dir, name+spool.PartialSuffix), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		c.cur = &file{f: f, name: name, acks: make([]subscriber.Ack, 0, min(c.maxEvents, 1<<16))}
		c.w.Reset(f)
	}
	// Every event has to be acked exactly once or its meta's watermark stops
	// moving, so the ack waits in the file's list until the file is safe.
	c.cur.acks = append(c.cur.acks, subscriber.Ack{MetaId: ev.GetMetaId(), SeqId: ev.GetSeqId()})
	// The whole event, v1 or v2, as Watch sent it. Reading it is downstream's job.
	if err := c.writeRecord(ev); err != nil {
		return err
	}
	if len(c.cur.acks) >= c.maxEvents {
		return c.rotate()
	}
	return nil
}

// writeRecord writes ev as one protodelim record: its length as a uvarint, then
// its bytes. It is what protodelim.MarshalTo writes, without that function's
// two allocations per call (a fresh slice for the message and one for the
// length): this runs once per event, at hundreds of thousands a second, so it
// allocates nothing once enc has grown to the largest event.
func (c *collector) writeRecord(ev *bw.Event) error {
	var err error
	if c.enc, err = (proto.MarshalOptions{}).MarshalAppend(c.enc[:0], ev); err != nil {
		return err
	}
	if _, err := c.w.Write(protowire.AppendVarint(c.size[:0], uint64(len(c.enc)))); err != nil {
		return err
	}
	_, err = c.w.Write(c.enc)
	return err
}

// rotate pushes the buffered tail into the file and hands the file to the
// sealer. It blocks only when the sealer is a whole queue of files behind.
func (c *collector) rotate() error {
	if c.cur == nil {
		return nil
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	c.sealq <- c.cur
	c.cur = nil
	return nil
}

// seal makes one file durable and then acks it: fsync the data, link it to its
// final name, fsync the directory so the name survives a power cut, and only
// then ack. Any other order can ack an event that is not on disk. It returns
// the per-meta highest sequence ID it acked.
func (c *collector) seal(fl *file, dir *os.File, high map[uint32]uint64) error {
	if err := fl.f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", fl.name, err)
	}
	if err := fl.f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", fl.name, err)
	}
	partial := filepath.Join(c.dir, fl.name+spool.PartialSuffix)
	// Link, not rename: rename silently replaces an existing file, and every
	// event in a sealed file has already been acked.
	if err := os.Link(partial, filepath.Join(c.dir, fl.name)); err != nil {
		return fmt.Errorf("seal %s: %w", fl.name, err)
	}
	if err := os.Remove(partial); err != nil {
		return fmt.Errorf("seal %s: %w", fl.name, err)
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsync spool directory: %w", err)
	}
	// One send per event until the SDK has AckUpTo (DESIGN.md D8); see file.
	for _, a := range fl.acks {
		c.acks <- a
		high[a.MetaId] = max(high[a.MetaId], a.SeqId)
	}
	c.log.Info("sealed", zap.String("file", fl.name), zap.Int("events", len(fl.acks)))
	return nil
}

// sealer seals files in the order they were written until sealq is closed.
// A failure is returned at once and nothing after it is acked.
func (c *collector) sealer(high map[uint32]uint64) error {
	dir, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	for fl := range c.sealq {
		if err := c.seal(fl, dir, high); err != nil {
			return err
		}
	}
	return nil
}

// run is the only reader of events, so the open file needs no lock. Events
// from every Watch arrive already tagged with a meta ID and are written in the
// order they arrive.
func (c *collector) run(ctx context.Context, every time.Duration) error {
	defer close(c.sealq)
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case ev := <-c.events:
			if err := c.add(ev); err != nil {
				return err
			}
		case <-tick.C:
			if err := c.rotate(); err != nil {
				return err
			}
		case <-ctx.Done():
			for {
				select {
				case ev := <-c.events:
					if err := c.add(ev); err != nil {
						return err
					}
				default:
					return c.rotate()
				}
			}
		}
	}
}

func main() {
	listen := flag.String("listen", "0.0.0.0:50052", "address Watch dials into")
	dir := flag.String("spool", "/var/lib/index-sync/spool", "spool directory for "+spool.Ext+" files")
	ckpt := flag.String("checkpoint", "/var/lib/index-sync/checkpoint.json", "acked sequence IDs")
	ackEvery := flag.Duration("checkpoint-every", time.Second, "how often acks reach Watch and disk; Watch frees buffer space only then")
	rollEvery := flag.Duration("roll-every", 30*time.Second, "seal the open file at least this often")
	// Every event in flight is unacked in Watch too, and Watch overwrites the
	// oldest unacked events once a meta's event-buffer-size fills. Sent but
	// unsealed events are then gone if the collector crashes. So keep
	// (seal-queue + 2) * roll-events well under event-buffer-size.
	rollEvents := flag.Int("roll-events", 100_000,
		"seal the open file once it holds this many events; (seal-queue + 2) times this is the most "+
			"held unacked, at 32 B each here and in the SDK, and must stay well under Watch's event-buffer-size")
	queue := flag.Int("queue", 65536, "events held between the gRPC streams and the writer")
	sealQueue := flag.Int("seal-queue", 4, "written files that may wait for fsync before receiving waits")
	noTLS := flag.Bool("tls-disable", true, "disable TLS, not for production")
	flag.Parse()

	log, _ := zap.NewProduction()
	defer log.Sync()

	// Watch frees buffer space only for what we have acked, and we ack only at
	// seal, so these bound how far behind Watch's buffer can get.
	if *rollEvents < 1 || *queue < 1 || *sealQueue < 1 || *rollEvery <= 0 {
		log.Fatal("roll-events, queue and seal-queue must be at least 1, roll-every above 0")
	}

	c := &collector{
		log:       log,
		dir:       *dir,
		maxEvents: *rollEvents,
		events:    make(chan *bw.Event, *queue),
		acks:      make(chan subscriber.Ack, 1<<16),
		sealq:     make(chan *file, *sealQueue),
		w:         bufio.NewWriterSize(nil, 1<<20),
	}
	if err := c.prepare(); err != nil {
		log.Fatal("could not prepare spool", zap.Error(err))
	}

	store, err := subscriber.NewDiskStore(*ckpt)
	if err != nil {
		log.Fatal("could not open checkpoint", zap.Error(err))
	}
	stored, err := store.Retrieve()
	if err != nil {
		log.Fatal("could not read checkpoint", zap.Error(err))
	}
	c.seqs = newSeqTracker(stored)
	server, err := subscriber.NewServer(log, subscriber.Config{
		Address:      *listen,
		TlsDisable:   *noTLS,
		AckFrequency: *ackEvery,
	}, store)
	if err != nil {
		log.Fatal("could not create server", zap.Error(err))
	}

	// A disk error ends the process. Carrying on would mean acking around a
	// file that never reached disk; exiting leaves those events unacked, so
	// they come back when the collector does.
	fatal := func(what string, err error) {
		if errors.Is(err, syscall.ENOSPC) {
			log.Fatal(what+": spool disk is full; events stay in Watch until it has room", zap.Error(err))
		}
		if errors.Is(err, fs.ErrExist) {
			log.Fatal(what+": a sealed file already has this name; refusing to replace it", zap.Error(err))
		}
		log.Fatal(what, zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	high := map[uint32]uint64{}
	received := make(chan struct{})
	sealed := make(chan struct{})
	go func() {
		if err := c.run(ctx, *rollEvery); err != nil {
			fatal("write failed", err)
		}
		close(received)
	}()
	go func() {
		if err := c.sealer(high); err != nil {
			fatal("seal failed", err)
		}
		close(sealed)
	}()

	errs := make(chan error, 1)
	go server.ListenAndServe(c.events, c.acks, errs)
	log.Info("collector started", zap.String("listen", *listen), zap.String("spool", *dir),
		zap.Int("roll-events", *rollEvents), zap.Duration("roll-every", *rollEvery))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errs:
		log.Error("subscriber server failed", zap.Error(err))
	case <-sig:
	}

	// Stop the source first so nothing new arrives, let the loop drain, then
	// let the sealer finish every file before acks close for the final flush.
	server.Stop()
	cancel()
	<-received
	<-sealed
	// The SDK records watermarks in the checkpoint from its per-stream
	// goroutines, which Stop has already ended, so the last files' acks would
	// never reach it and would replay after a restart. Everything received has
	// now been sealed, so each meta's highest acked ID is safe to record. Only
	// raise an entry, or replace the SDK's "seek to end" placeholder (MaxUint64)
	// for a meta it had not checkpointed yet.
	if stored, err := store.Retrieve(); err == nil {
		for meta, seq := range high {
			if old, ok := stored[meta]; !ok || old == math.MaxUint64 || seq > old {
				store.Store(meta, seq)
			}
		}
	}
	close(c.acks)
	server.WaitFlushed()
	log.Info("collector stopped")
}
