// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/elastic/elastic-agent-shipper-client/pkg/helpers"
	pb "github.com/elastic/elastic-agent-shipper-client/pkg/proto"
	"github.com/elastic/elastic-agent-shipper-client/pkg/proto/messages"
	"github.com/elastic/elastic-agent-shipper/queue"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const bufSize = 1024 * 1024 // 1MB

func TestPublish(t *testing.T) {
	ctx := context.Background()

	sampleValues, err := helpers.NewStruct(map[string]interface{}{
		"string": "value",
		"number": 42,
	})

	require.NoError(t, err)

	e := &messages.Event{
		Timestamp: timestamppb.Now(),
		Source: &messages.Source{
			InputId:  "input",
			StreamId: "stream",
		},
		DataStream: &messages.DataStream{
			Type:      "log",
			Dataset:   "default",
			Namespace: "default",
		},
		Metadata: sampleValues,
		Fields:   sampleValues,
	}

	publisher := &publisherMock{
		persistedIndex: 42,
	}
	shipper, err := NewShipperServer(publisher)
	defer func() { _ = shipper.Close() }()
	require.NoError(t, err)
	client, stop := startServer(t, ctx, shipper)
	defer stop()

	// get the current UUID
	pirCtx, cancel := context.WithCancel(ctx)
	consumer, err := client.PersistedIndex(pirCtx, &messages.PersistedIndexRequest{})
	require.NoError(t, err)
	pir, err := consumer.Recv()
	require.NoError(t, err)
	cancel() // close the stream

	t.Run("should successfully publish a batch", func(t *testing.T) {
		publisher.q = make([]*messages.Event, 0, 3)
		events := []*messages.Event{e, e, e}
		reply, err := client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   pir.Uuid,
			Events: events,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(len(events)), reply.AcceptedCount)
		require.Equal(t, uint64(len(events)), reply.AcceptedIndex)
		require.Equal(t, uint64(publisher.persistedIndex), pir.PersistedIndex)
	})

	t.Run("should grow accepted index", func(t *testing.T) {
		publisher.q = make([]*messages.Event, 0, 3)
		events := []*messages.Event{e}
		reply, err := client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   pir.Uuid,
			Events: events,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(len(events)), reply.AcceptedCount)
		require.Equal(t, uint64(1), reply.AcceptedIndex)
		require.Equal(t, uint64(publisher.persistedIndex), pir.PersistedIndex)
		reply, err = client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   pir.Uuid,
			Events: events,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(len(events)), reply.AcceptedCount)
		require.Equal(t, uint64(2), reply.AcceptedIndex)
		require.Equal(t, uint64(publisher.persistedIndex), pir.PersistedIndex)
		reply, err = client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   pir.Uuid,
			Events: events,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(len(events)), reply.AcceptedCount)
		require.Equal(t, uint64(3), reply.AcceptedIndex)
		require.Equal(t, uint64(publisher.persistedIndex), pir.PersistedIndex)
	})

	t.Run("should return different count when queue is full", func(t *testing.T) {
		publisher.q = make([]*messages.Event, 0, 1)
		events := []*messages.Event{e, e, e} // 3 should not fit into the queue size 1
		reply, err := client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   pir.Uuid,
			Events: events,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(1), reply.AcceptedCount)
		require.Equal(t, uint64(1), reply.AcceptedIndex)
		require.Equal(t, uint64(publisher.persistedIndex), pir.PersistedIndex)
	})

	t.Run("should return an error when uuid does not match", func(t *testing.T) {
		publisher.q = make([]*messages.Event, 0, 3)
		events := []*messages.Event{e, e, e}
		reply, err := client.PublishEvents(ctx, &messages.PublishRequest{
			Uuid:   "wrong",
			Events: events,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "UUID does not match")
		require.Nil(t, reply)
	})
}

func TestPersistedIndex(t *testing.T) {
	ctx := context.Background()

	publisher := &publisherMock{persistedIndex: 42}

	t.Run("server should send updates to the clients", func(t *testing.T) {
		shipper, err := NewShipperServer(publisher)
		defer func() { _ = shipper.Close() }()
		require.NoError(t, err)
		client, stop := startServer(t, ctx, shipper)
		defer stop()

		// first delivery can happen before the first index update
		require.Eventually(t, func() bool {
			cl := createConsumers(t, ctx, client, 5, 5*time.Millisecond)
			defer cl.stop()
			return cl.assertConsumed(t, 42) // initial value in the publisher
		}, 100*time.Millisecond, time.Millisecond, "clients are supposed to get the update")

		cl := createConsumers(t, ctx, client, 50, 5*time.Millisecond)
		publisher.persistedIndex = 64

		cl.assertConsumed(t, 64)

		publisher.persistedIndex = 128

		cl.assertConsumed(t, 128)

		cl.stop()
	})

	t.Run("server should properly shutdown", func(t *testing.T) {
		shipper, err := NewShipperServer(publisher)
		require.NoError(t, err)
		client, stop := startServer(t, ctx, shipper)
		defer stop()

		cl := createConsumers(t, ctx, client, 50, 5*time.Millisecond)
		publisher.persistedIndex = 64
		shipper.Close() // stopping the server
		require.Eventually(t, func() bool {
			return cl.assertClosedServer(t) // initial value in the publisher
		}, 100*time.Millisecond, time.Millisecond, "server was supposed to shutdown")
	})
}

func startServer(t *testing.T, ctx context.Context, shipperServer ShipperServer) (client pb.ProducerClient, stop func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()

	pb.RegisterProducerServer(grpcServer, shipperServer)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		require.NoError(t, err)
	}

	stop = func() {
		shipperServer.Close()
		conn.Close()
		grpcServer.Stop()
	}

	return pb.NewProducerClient(conn), stop
}

func createConsumers(t *testing.T, ctx context.Context, client pb.ProducerClient, count int, pollingInterval time.Duration) consumerList {
	ctx, cancel := context.WithCancel(ctx)

	cl := consumerList{
		stop:      cancel,
		consumers: make([]pb.Producer_PersistedIndexClient, 0, count),
	}
	for i := 0; i < 50; i++ {
		consumer, err := client.PersistedIndex(ctx, &messages.PersistedIndexRequest{
			PollingInterval: durationpb.New(pollingInterval),
		})
		require.NoError(t, err)
		cl.consumers = append(cl.consumers, consumer)
	}

	return cl
}

type consumerList struct {
	consumers []pb.Producer_PersistedIndexClient
	stop      func()
}

func (l consumerList) assertConsumed(t *testing.T, value uint64) bool {
	for _, c := range l.consumers {
		pir, err := c.Recv()
		require.NoError(t, err)
		if pir.PersistedIndex != value {
			return false
		}
	}
	return true
}

func (l consumerList) assertClosedServer(t *testing.T) bool {
	for _, c := range l.consumers {
		_, err := c.Recv()
		if err == nil {
			return false
		}

		if !strings.Contains(err.Error(), "server is stopped: context canceled") {
			return false
		}
	}

	return true
}

type publisherMock struct {
	Publisher
	q              []*messages.Event
	persistedIndex queue.EntryID
}

func (p *publisherMock) Publish(event *messages.Event) (queue.EntryID, error) {
	if len(p.q) == cap(p.q) {
		return queue.EntryID(0), queue.ErrQueueIsFull
	}

	p.q = append(p.q, event)
	return queue.EntryID(len(p.q)), nil
}

func (p *publisherMock) AcceptedIndex() queue.EntryID {
	return queue.EntryID(len(p.q))
}

func (p *publisherMock) PersistedIndex() queue.EntryID {
	return p.persistedIndex
}
