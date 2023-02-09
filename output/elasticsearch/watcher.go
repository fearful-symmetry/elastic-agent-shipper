// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package elasticsearch

import (
	"context"
	"sync"
	"time"
)

// WatchState represents the state of the output at the time the WatchReporter callback was made
type WatchState string

var WATCH_DEGRADED WatchState = "degraded"
var WATCH_RECOVERED WatchState = "recovered"

// WatchReporter is a callback that the watcher will call whenever the output has reached a degraded state, or recovered from a degraded state
type WatchReporter func(state WatchState, msg string)

// ESHeathWatcher monitors the ES connection and notifies if a failure persists for more than seconds
type ESHeathWatcher struct {
	reporter        WatchReporter
	lastSuccess     time.Time
	lastFailure     time.Time
	didFail         bool
	lastFailMessage string
	failureInterval time.Duration
	waitInterval    time.Duration

	mut sync.Mutex
}

func newHealthWatcher(reporter WatchReporter, failureInterval time.Duration) ESHeathWatcher {
	return ESHeathWatcher{
		reporter:        reporter,
		didFail:         false,
		failureInterval: failureInterval,
		waitInterval:    time.Second,
	}
}

func (hw *ESHeathWatcher) Fail(msg string) {
	hw.mut.Lock()
	defer hw.mut.Unlock()
	hw.lastFailure = time.Now()
	hw.lastFailMessage = msg
}

func (hw *ESHeathWatcher) Success() {
	hw.mut.Lock()
	defer hw.mut.Unlock()
	hw.lastSuccess = time.Now()
}

// Watch starts a blocking loop and will report if there has been a failure for longer than a given period
func (hw *ESHeathWatcher) Watch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		hw.mut.Lock()
		// if the last success was > failure interval, and a failure was more recent, then report
		if time.Now().Sub(hw.lastSuccess) > hw.failureInterval && hw.lastFailure.After(hw.lastSuccess) && !hw.didFail {
			hw.didFail = true
			if hw.reporter != nil {
				hw.reporter(WATCH_DEGRADED, hw.lastFailMessage)
			}

		}
		if time.Now().Sub(hw.lastSuccess) < hw.failureInterval && hw.didFail {
			hw.didFail = false
			if hw.reporter != nil {
				hw.reporter(WATCH_RECOVERED, "ES is sending events")
			}
		}
		hw.mut.Unlock()
		time.Sleep(hw.waitInterval)
	}
}
