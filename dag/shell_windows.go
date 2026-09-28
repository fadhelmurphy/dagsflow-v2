//go:build windows

package dag

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// CommandContext starts the process in its own process group; cancelling
// (timeout, manual mark, scheduler shutdown) kills the whole process tree
// with taskkill /T, so children such as python or the bq CLI do not linger.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP, HideWindow: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		kill := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
		kill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		if err := kill.Run(); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

// Shell resolution for bash tasks on Windows, in order:
//  1. DAGSFLOW_SHELL: "bash", "pwsh", "powershell", "cmd" or a path to bash.exe
//  2. Git Bash (bash.exe from Git for Windows), so bash-style commands work as on Linux
//  3. cmd.exe
//
// C:\Windows\System32\bash.exe is the WSL launcher and is skipped: it would run
// the command inside WSL with a different filesystem and environment.
var (
	shellOnce sync.Once
	shellArgv []string
)

func findGitBash() string {
	if p, err := exec.LookPath("bash"); err == nil && !isWSLLauncher(p) {
		return p
	}
	for _, env := range []string{"ProgramFiles", "ProgramW6432", "ProgramFiles(x86)", "LOCALAPPDATA"} {
		base := os.Getenv(env)
		if base == "" {
			continue
		}
		for _, rel := range []string{`Git\bin\bash.exe`, `Programs\Git\bin\bash.exe`} {
			p := filepath.Join(base, rel)
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	return ""
}

func isWSLLauncher(p string) bool {
	p = strings.ToLower(filepath.Clean(p))
	return strings.HasSuffix(p, `\system32\bash.exe`) || strings.Contains(p, `\windowsapps\`)
}

func resolveShell() []string {
	shellOnce.Do(func() {
		switch s := strings.TrimSpace(os.Getenv("DAGSFLOW_SHELL")); strings.ToLower(s) {
		case "cmd":
			shellArgv = []string{"cmd", "/C"}
		case "pwsh", "powershell":
			shellArgv = []string{s, "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"}
		case "", "bash":
			if b := findGitBash(); b != "" {
				shellArgv = []string{b, "-c"}
			} else {
				shellArgv = []string{"cmd", "/C"}
			}
		default: // a path to a shell that accepts -c (e.g. bash.exe, sh.exe)
			shellArgv = []string{s, "-c"}
		}
	})
	return shellArgv
}

// ShellName reports which shell bash tasks use (logged at task start).
func ShellName() string { return strings.Join(resolveShell()[:1], "") }

func shellCommand(ctx context.Context, script string) *exec.Cmd {
	argv := resolveShell()
	if strings.EqualFold(argv[0], "cmd") {
		// cmd.exe does its own quote parsing: pass the command line verbatim
		cmd := CommandContext(ctx, "cmd")
		cmd.SysProcAttr.CmdLine = `cmd /S /C "` + script + `"`
		return cmd
	}
	args := append(append([]string{}, argv[1:]...), script)
	return CommandContext(ctx, argv[0], args...)
}
