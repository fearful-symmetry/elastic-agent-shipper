// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/elastic/elastic-agent-client/v7/pkg/client"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-shipper/config"
)

// LoadAndRun loads the config object and runs the gRPC server
func LoadAndRun() error {
	cfg, err := config.ReadConfigFromFile()
	switch {
	case err == nil:
		return RunUnmanaged(cfg)
	case errors.Is(err, config.ErrConfigIsNotSet):
		return RunManaged(cfg)
	default:
		return err
	}
}

// RunUnmanaged runs the shipper out of a local config file without using the control protocol.
func RunUnmanaged(cfg config.ShipperConfig) error {
	log := logp.L()
	runner, err := NewServerRunner(cfg)
	if err != nil {
		return err
	}
	done := make(doneChan)
	go func() {
		err = runner.Start()
		done <- struct{}{}
	}()

	// On termination signals, gracefully stop the shipper
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	sig := <-sigc
	switch sig {
	case syscall.SIGINT, syscall.SIGTERM:
		log.Debug("Received sigterm/sigint, stopping")
	case syscall.SIGHUP:
		log.Debug("Received sighup, stopping")
	}

	_ = runner.Close()

	<-done

	return err
}

// RunManaged runs the shipper receiving configuration from the agent using the control protocol.
func RunManaged(cfg config.ShipperConfig) error {
	agentClient, _, err := client.NewV2FromReader(os.Stdin, client.VersionInfo{Name: "elastic-agent-shipper", Version: "v2"})
	if err != nil {
		return fmt.Errorf("error reading control config from agent: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = runController(ctx, agentClient)

	return err
}
<<<<<<< HEAD

// handle shutdown of the shipper
func handleShutdown(stopFunc func(), done doneChan) {
	var callback sync.Once
	log := logp.L()
	// On termination signals, gracefully stop the Beat
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-done:
				log.Debugf("Shutting down from agent controller")
				callback.Do(stopFunc)
				return
			case sig := <-sigc:
				switch sig {
				case syscall.SIGINT, syscall.SIGTERM:
					log.Debug("Received sigterm/sigint, stopping")
				case syscall.SIGHUP:
					log.Debug("Received sighup, stopping")
				}
				callback.Do(stopFunc)
				return
			}
		}

	}()
}

// Run starts the gRPC server
func (c *clientHandler) Run(cfg config.ShipperConfig, unit *client.Unit) error {
	log := logp.L()

	// When there is queue-specific configuration in ShipperConfig, it should
	// be passed in here.
	queue, err := queue.New(cfg.Queue)
	if err != nil {
		return fmt.Errorf("couldn't create queue: %w", err)
	}

	// Make a placeholder console output to read the queue's events
	out := output.NewConsole(queue)
	out.Start()

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Server.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	monHandler, err := loadMonitoring(cfg, queue)
	if err != nil {
		return fmt.Errorf("error loading outputs: %w", err)
	}
	unit.RegisterDiagnosticHook("queue", "queue metrics", "", "application/json", monHandler.DiagnosticsCallback())

	_ = unit.UpdateState(client.UnitStateConfiguring, "starting shipper server", nil)

	var opts []grpc.ServerOption
	if cfg.Server.TLS {
		creds, err := credentials.NewServerTLSFromFile(cfg.Server.Cert, cfg.Server.Key)
		if err != nil {
			return fmt.Errorf("failed to generate credentials %w", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	shipperServer, err := server.NewShipperServer(cfg.Server, queue)
	if err != nil {
		return fmt.Errorf("failed to initialise the server: %w", err)
	}
	pb.RegisterProducerServer(grpcServer, shipperServer)

	shutdownFunc := func() {
		grpcServer.GracefulStop()
		monHandler.End()
		queue.Close()
		// The output will shut down once the queue is closed.
		// We call Wait to give it a chance to finish with events
		// it has already read.
		out.Wait()
		shipperServer.Close()
	}
	handleShutdown(shutdownFunc, c.shutdownInit)
	log.Debugf("gRPC server is listening on port %d", cfg.Server.Port)
	_ = unit.UpdateState(client.UnitStateHealthy, "Shipper Running", nil)

	// This will get sent after the server has shutdown, signaling to the runloop that it can stop.
	// The shipper has no queues connected right now, but once it does, this function can't run until
	// after the queues have emptied and/or shutdown. We'll presumably have a better idea of how this
	// will work once we have queues connected here.
	defer func() {
		log.Debugf("shipper has completed shutdown, stopping")
		c.shutdownComplete.Done()
	}()
	c.shutdownComplete.Add(1)
	return grpcServer.Serve(lis)

}

// Initialize metrics and outputs
func loadMonitoring(cfg config.ShipperConfig, queue *queue.Queue) (*monitoring.QueueMonitor, error) {
	//startup monitor
	mon, err := monitoring.NewFromConfig(cfg.Monitor, queue)
	if err != nil {
		return nil, fmt.Errorf("error initializing output monitor: %w", err)
	}

	mon.Watch()

	return mon, nil
}
=======
>>>>>>> upstream/main
