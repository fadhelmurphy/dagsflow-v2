//go:build windows

package cmd

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	getConsoleMode = kernel32.NewProc("GetConsoleMode")
	setConsoleMode = kernel32.NewProc("SetConsoleMode")
)

const enableEchoInput = 0x0004

// withEchoOff runs read with console echo disabled (cmd, PowerShell, Windows
// Terminal); reports whether echo was actually disabled.
func withEchoOff(read func()) bool {
	h := uintptr(os.Stdin.Fd())
	var mode uint32
	if r, _, _ := getConsoleMode.Call(h, uintptr(unsafe.Pointer(&mode))); r == 0 {
		read() // not a console (pipe, Git Bash mintty): nothing to hide
		return false
	}
	setConsoleMode.Call(h, uintptr(mode&^enableEchoInput))
	defer setConsoleMode.Call(h, uintptr(mode))
	read()
	return true
}
