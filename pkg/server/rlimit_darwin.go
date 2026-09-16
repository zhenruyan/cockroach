// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !windows && !freebsd && !dragonfly
// +build !windows,!freebsd,!dragonfly

package server

import (
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func setRlimitNoFile(limits *rlimit) error {
	return unix.Setrlimit(unix.RLIMIT_NOFILE, (*unix.Rlimit)(limits))
}

func getRlimitNoFile(limits *rlimit) error {
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, (*unix.Rlimit)(limits)); err != nil {
		return err
	}
	// On macOS, the true hard open file limit is
	// min(sysctl("kern.maxfiles"),
	//     sysctl("kern.maxfilesperproc"),
	//     getrlimit(RLIMIT_NOFILE))
	// This does not appear to be documented and may be incomplete.
	//
	// See https://github.com/golang/go/issues/30401 for more context.
	sysctlMaxFiles, err := getSysctlMaxFiles("kern.maxfiles")
	if err != nil {
		return err
	}
	if limits.Max > sysctlMaxFiles {
		limits.Max = sysctlMaxFiles
	}
	sysctlMaxFilesPerProc, err := getSysctlMaxFiles("kern.maxfilesperproc")
	if err != nil {
		return err
	}
	if limits.Max > sysctlMaxFilesPerProc {
		limits.Max = sysctlMaxFilesPerProc
	}
	return nil
}

// getSysctlMaxFiles reads an integer-valued sysctl (e.g. "kern.maxfiles")
// without requiring cgo.
func getSysctlMaxFiles(name string) (uint64, error) {
	s, err := unix.Sysctl(name)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}
