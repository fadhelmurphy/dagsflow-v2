//go:build windows

package cmd

import (
	"os"
	"os/exec"
	"syscall"
	"time"

	"dagsflow-go/engine"
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

// terminate: Windows has no SIGTERM, so the scheduler is asked to stop via a
// stop file it checks every tick (graceful: task processes are stopped and
// running runs resume on the next start). Killed only if it does not exit.
func terminate(st *engine.Store, pid int) error {
	if err := engine.RequestStop(st); err != nil {
		return err
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := engine.SchedulerInfo(st); !alive {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}
