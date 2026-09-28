//go:build !windows

package cmd

import (
	"os"
	"os/exec"
	"syscall"
)

func spawnDetached(logFile *os.File, args ...string) (int, error) {
	c := exec.Command(os.Args[0], args...)
	c.Stdout, c.Stderr = logFile, logFile
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := c.Start(); err != nil {
		return 0, err
	}
	pid := c.Process.Pid
	return pid, c.Process.Release()
}

// terminate sends SIGTERM so the scheduler can stop gracefully.
func terminate(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Signal(syscall.SIGTERM)
}
