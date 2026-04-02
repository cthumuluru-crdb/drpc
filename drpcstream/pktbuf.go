// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcstream

// defaultPacketBufferSize is the number of messages the packetBuffer can
// hold before the producer blocks. This decouples the transport reader
// from the consumer (RPC handler), preventing deadlocks when the handler
// is delayed before calling Recv.
const defaultPacketBufferSize = 10

type packetBuffer struct {
	q *spscQueue
}

func (pb *packetBuffer) init() {
	pb.q = newSPSCQueue(defaultPacketBufferSize)
}

func (pb *packetBuffer) Close(err error) {
	pb.q.Close(err)
}

func (pb *packetBuffer) Put(data []byte) {
	pb.q.Enqueue(data)
}

func (pb *packetBuffer) Get() ([]byte, error) {
	return pb.q.Dequeue()
}

func (pb *packetBuffer) Done() {
	pb.q.Done()
}
