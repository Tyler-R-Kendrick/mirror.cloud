//go:build unix

package execution

import (
	"os/exec"
	"syscall"
)

// setProcessGroup starts cmd in a process group of its own, so killGroup can
// stop it together with everything it starts.
func setProcessGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

// killGroup sends sig to the process group led by pid.
func killGroup(pid int, sig syscall.Signal) { _ = syscall.Kill(-pid, sig) }
