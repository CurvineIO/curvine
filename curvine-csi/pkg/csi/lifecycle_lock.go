/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"sync"
)

// keyedLifecycleLocks serializes one mount's lifecycle without blocking other
// mounts. Waiters can cancel. Entries live until both holder and waiters leave.
// Lock order: lifecycle -> persistence -> state mutex (short memory access).
type keyedLifecycleLocks struct {
	mu      sync.Mutex
	entries map[string]*lifecycleLock
	// Optional immutable resource identity, set before the manager is published.
	keyFor func(string) string
}

type lifecycleLock struct {
	gate  chan struct{}
	users int
}

func (l *keyedLifecycleLocks) acquire(ctx context.Context, key string) (func(), error) {
	if l.keyFor != nil {
		key = l.keyFor(key)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[string]*lifecycleLock)
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &lifecycleLock{gate: make(chan struct{}, 1)}
		l.entries[key] = entry
	}
	entry.users++
	l.mu.Unlock()
	forget := func() {
		l.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(l.entries, key)
		}
		l.mu.Unlock()
	}
	select {
	case entry.gate <- struct{}{}:
		unlock := func() { <-entry.gate; forget() }
		if err := ctx.Err(); err != nil {
			unlock()
			return nil, err
		}
		return unlock, nil
	case <-ctx.Done():
		forget()
		return nil, ctx.Err()
	}
}
