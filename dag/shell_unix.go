//go:build !windows

package dag

import (
	"context"
	"os/exec"
	"syscall"
	"time"
)

// CommandContext is exec.CommandContext that starts the process in its own
// process group, so cancelling (timeout / shutdown) kills the whole tree.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

// ShellName reports which shell bash tasks use (logged at task start).
func ShellName() string { return "bash" }

func shellCommand(ctx context.Context, script string) *exec.Cmd {
	return CommandContext(ctx, "bash", "-c", script)
}
