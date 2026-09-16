// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build linux
// +build linux

package ptp

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

// This file provides the non-cgo Linux implementation of the PTP clock
// interface; the real implementation (ptp_clock_linux.go) uses cgo ioctls.

// Clock reads the time from a ptp device. Only implemented with cgo on Linux.
type Clock struct{}

// MakeClock is not available in binaries built without cgo.
func MakeClock(_ context.Context, _ string) (Clock, error) {
	return Clock{}, errors.New("clock device not supported in this build (CGO_ENABLED=0)")
}

// Now implements the hlc.WallClock interface.
func (p Clock) Now() time.Time {
	panic(errors.New("clock device not supported in this build (CGO_ENABLED=0)"))
}
