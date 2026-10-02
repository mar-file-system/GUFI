# Watch subscriber examples

A small Go program that reads the BeeGFS Watch event stream and prints it, to show the
protocol. The collector, markwalk and the spool reader built on it live in `beegfs_watch/`.

It is not part of the CMake build. It is a separate Go module so it can be built and run on
its own.

```bash
cd examples/watch-subscriber
go mod tidy
```

## subscriber/

About eighty lines. It prints every event it receives and nothing else.

Watch dials **out** to you, so this program is the gRPC server and Watch is the client. The
whole contract is one bidirectional stream: Watch sends `Event`, you send `Response`. A
`Response` acknowledges a sequence id, and the first one after connecting may carry an
`EventFilter`. An empty filter means every type, which is what you want while learning.

```bash
go run ./subscriber --listen 0.0.0.0:50052
```

Then make something happen on the mount. Events appear a moment later.

## Next steps

`beegfs_watch/` turns the stream into input for GUFI's incremental update:

| program | what it does | design |
|---|---|---|
| `collector/` | receives Watch events and seals them into numbered segment files | `DESIGN.md` |
| `markwalk/` | turns each batch of segments into a `<inode> d` suspect file | `markwalk/DESIGN.md`, `markwalk/FLOW.md` |

No cluster to hand? `go run ./markwalk -demo` (from `beegfs_watch/`) builds small trees in a
temp directory and shows what the walk marks in five awkward cases.
