//go:build windows

package main

import (
	"syscall"
	"time"
)

// cpuTime is this process's user+system CPU time so far.
func cpuTime() time.Duration {
	h, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0
	}
	var creation, exit, kernel, user syscall.Filetime
	if err := syscall.GetProcessTimes(h, &creation, &exit, &kernel, &user); err != nil {
		return 0
	}
	// FILETIME durations count 100 ns intervals.
	ticks := func(ft syscall.Filetime) int64 { return int64(ft.HighDateTime)<<32 | int64(ft.LowDateTime) }
	return time.Duration((ticks(kernel) + ticks(user)) * 100)
}
