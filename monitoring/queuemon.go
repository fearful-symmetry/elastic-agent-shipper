// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package monitoring

import (
	"fmt"
	"time"

	outqueue "github.com/elastic/beats/v7/libbeat/publisher/queue"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/opt"

	"github.com/elastic/elastic-agent-shipper/monitoring/reporter"

	//import the expvar queue metrics output
	_ "github.com/elastic/elastic-agent-shipper/monitoring/reporter/expvar"
	//import the log queue metrics output
	_ "github.com/elastic/elastic-agent-shipper/monitoring/reporter/log"
)

//QueueMonitor is the main handler object for the queue monitor, and will be responsible for startup, shutdown, handling config, and persistent tracking of metrics.
type QueueMonitor struct {
	// An array of user-configured outputs
	outputs []reporter.Reporter
	//user-configured reporting interval
	interval time.Duration
	done     chan struct{}
	// handler for the event queue
	queue outqueue.Queue
	log   *logp.Logger

	// Count of times the queue has reached a configured limit.
	queueLimitCount uint64
}

//Config is the intermediate struct representation of the queue monitor config
type Config struct {
	OutputConfig []config.Namespace `config:"outputs"`
	Interval     time.Duration      `config:"interval"`
	Enabled      bool               `config:"enabled"`
}

// DefaultConfig returns the default settings for the queue monitor
func DefaultConfig() Config {
	return Config{
		OutputConfig: nil,
		Enabled:      true,
		Interval:     time.Second * 30,
	}
}

// NewFromConfig creates a new queue monitor from a pre-filled config struct.
func NewFromConfig(cfg Config, queue outqueue.Queue) (*QueueMonitor, error) {
	if !cfg.Enabled {
		return &QueueMonitor{}, nil
	}
	//init outputs
	outputs, err := initOutputs(cfg)
	if err != nil {
		return nil, fmt.Errorf("error initializing queue metrics outputs: %w", err)
	}
	return &QueueMonitor{
		interval: cfg.Interval,
		queue:    queue,
		done:     make(chan struct{}),
		log:      logp.L(),
		outputs:  outputs,
	}, nil
}

// Watch is a non-blocking call that starts up a queue watcher that will report metrics to a given output
func (mon QueueMonitor) Watch() {
	// Turn this function into a no-op if nothing is initialized.
	if mon.queue == nil {
		return
	}
	ticker := time.NewTicker(mon.interval)
	mon.log.Debugf("Starting queue metrics watcher.")
	go func() {
		for {
			select {
			case <-mon.done:
				return
			case <-ticker.C:
				err := mon.updateMetrics()
				if err != nil {
					mon.log.Errorf("Error updating metrics: %w", err)
				}
			}
		}
	}()
}

// End closes the watcher
func (mon QueueMonitor) End() {
	mon.done <- struct{}{}
}

// updateMetrics is responsible for fetching the metrics from the queue, calculating whatever it needs to, and sending the complete events to the output
func (mon *QueueMonitor) updateMetrics() error {
	raw, err := mon.queue.Metrics()
	if err != nil {
		return fmt.Errorf("error fetching queue Metrics: %w", err)
	}

	count, limit, queueIsFull, err := getLimits(raw)
	if err != nil {
		return fmt.Errorf("could not get limits: %w", err)
	}

	if queueIsFull {
		mon.queueLimitCount = mon.queueLimitCount + 1
	}

	mon.sendToOutputs(reporter.QueueMetrics{
		CurrentQueueLevel:      opt.UintWith(count),
		QueueMaxLevel:          opt.UintWith(limit),
		QueueIsCurrentlyFull:   queueIsFull,
		QueueLimitReachedCount: opt.UintWith(mon.queueLimitCount),
		// Running on a philosophy that the outputs should be dumb and unopinionated,
		//so we're doing the type conversion here.
		OldestActiveEvent: raw.OldestActiveTimestamp.String(),
	})

	return nil
}

func (mon QueueMonitor) sendToOutputs(evt reporter.QueueMetrics) {
	for _, out := range mon.outputs {
		err := out.Update(evt)
		//Assuming we don't want to make this a hard error, since one broken output doesn't mean they're all broken.
		if err != nil {
			mon.log.Errorf("Error sending to output: %w", err)
		}
	}

}

// Load the raw config and look for monitoring outputs to initialize.
func initOutputs(cfg Config) ([]reporter.Reporter, error) {
	outputList := []reporter.Reporter{}

	if cfg.OutputConfig == nil {
		logger, err := reporter.OutputForName("log")
		if err != nil {
			return nil, fmt.Errorf("no default logger 'log' registered")
		}
		// logging output doesn't have a config
		def, err := logger(config.Namespace{})
		if err != nil {
			return nil, fmt.Errorf("error starting up default metrics logger: %w", err)
		}
		return []reporter.Reporter{def}, nil
	}

	for _, output := range cfg.OutputConfig {
		init, err := reporter.OutputForName(output.Name())
		if err != nil {
			return nil, fmt.Errorf("error finding output %s: %w", output.Name(), err)
		}
		reporter, err := init(output)
		if err != nil {
			return nil, fmt.Errorf("error initializing output %s: %w", output.Name(), err)
		}
		outputList = append(outputList, reporter)
	}
	return outputList, nil
}

// This is a wrapper to deal with the multiple queue metric "types",
// as we could either be dealing with event counts, or bytes.
// The reporting interfaces assumes we only want one.
func getLimits(raw outqueue.Metrics) (uint64, uint64, bool, error) {

	//Can some queues have both? Bias towards event count, since it's a little simpler.
	if raw.EventCount.Exists() && raw.EventLimit.Exists() {
		count := raw.EventCount.ValueOr(0)
		limit := raw.EventLimit.ValueOr(0)
		return count, limit, count >= limit, nil
	}

	if raw.ByteCount.Exists() && raw.ByteLimit.Exists() {
		count := raw.ByteCount.ValueOr(0)
		limit := raw.ByteLimit.ValueOr(0)
		// As @faec has noted, calculating limits can be a bit awkward when we're dealing with reporting/configuration in bytes.
		//All we have now is total queue size in bytes, which doesn't tell us how many more events could fit before we hit the queue limit.
		//So until we have something better, mark anything as 90% or more full as "full"
		level := float64(count) / float64(limit)
		return count, limit, level > 0.9, nil
	}

	return 0, 0, false, fmt.Errorf("could not find valid byte or event metrics in queue")
}
