//go:build !windows

package cmd

import (
	"os"
	"os/exec"
)

// withEchoOff runs read with terminal echo disabled (stty); reports whether
// echo was actually disabled.
func withEchoOff(read func()) bool {
	off := exec.Command("stty", "-echo")
	off.Stdin = os.Stdin
	if off.Run() != nil {
		read()
		return false
	}
	defer func() {
		on := exec.Command("stty", "echo")
		on.Stdin = os.Stdin
		_ = on.Run()
	}()
	read()
	return true
}
