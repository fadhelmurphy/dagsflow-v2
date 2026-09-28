//go:build windows

package cmd

import (
	"os"
	"os/exec"
	"syscall"
)

func spawnDetached(logFile *os.File, args ...string) (int, error) {
	c := exec.Command(os.Args[0], args...)
	c.Stdout, c.Stderr = logFile, logFile
	c.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
	if err := c.Start(); err != nil {
		return 0, err
	}
	pid := c.Process.Pid
	return pid, c.Process.Release()
}

// terminate: Windows has no SIGTERM; running runs resume on next scheduler start.
func terminate(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}
