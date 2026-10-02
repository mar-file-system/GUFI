// Minimal BeeGFS Watch subscriber: prints every event it receives.
//
// Watch dials OUT to this process, so we are the gRPC server and Watch is the client.
// Nothing here filters: an empty EventFilter means "send me everything".
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	bw "github.com/thinkparq/protobuf/go/beewatch"
	"google.golang.org/grpc"
)

type server struct {
	bw.UnimplementedSubscriberServer
}

// ReceiveEvents is the only RPC. Watch streams Events in; we stream Responses back
// to acknowledge them. The first Response may carry an EventFilter.
func (s *server) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {
	log.Println("Watch connected")

	// Ack with no filter -> we get every event type.
	if err := stream.Send(&bw.Response{CompletedSeq: 0}); err != nil {
		return err
	}

	var last uint64
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			log.Println("Watch closed the stream")
			return nil
		}
		if err != nil {
			log.Printf("stream error: %v", err)
			return err
		}

		v2 := ev.GetV2()
		if v2 == nil {
			continue // a v1 (BeeGFS 7) event; ignore for this demo
		}

		when := time.Unix(0, v2.GetTimestamp()).Format("15:04:05.000")
		line := fmt.Sprintf("%s  seq=%-6d meta=%d uid=%-5d %-22s %s",
			when, ev.GetSeqId(), ev.GetMetaId(), v2.GetMsgUserId(), v2.GetType(), v2.GetPath())
		if t := v2.GetTargetPath(); t != "" {
			line += "  ->  " + t
		}
		fmt.Println(line)

		// Acknowledge. Watch will not replay anything at or below this sequence id.
		last = ev.GetSeqId()
		if err := stream.Send(&bw.Response{CompletedSeq: last}); err != nil {
			return err
		}
	}
}

func main() {
	addr := flag.String("listen", "0.0.0.0:50052", "address Watch should dial")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	g := grpc.NewServer()
	bw.RegisterSubscriberServer(g, &server{})

	log.Printf("listening on %s, waiting for Watch to dial in", *addr)
	if err := g.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
