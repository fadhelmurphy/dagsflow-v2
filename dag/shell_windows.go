//go:build windows

package dag

import (
	"context"
	"os/exec"
	"time"
)

func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func shellCommand(ctx context.Context, script string) *exec.Cmd {
	return CommandContext(ctx, "cmd", "/C", script)
}
