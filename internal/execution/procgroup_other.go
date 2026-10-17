//go:build !unix

package execution

import (
	"os"
	"os/exec"
	"syscall"
)

// There are no POSIX process groups here: the runtime process alone is
// stopped, and always by killing it, since a graceful signal cannot be sent.
// The child processes it started may outlive it.
func setProcessGroup(*exec.Cmd) {}

func killGroup(pid int, _ syscall.Signal) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}
