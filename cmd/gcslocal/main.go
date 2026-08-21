// Command gcslocal wraps gcloud/gsutil-style env against the local emulator.
package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	host := os.Getenv("STORAGE_EMULATOR_HOST")
	if host == "" {
		host = "127.0.0.1:4566"
	}
	bin := "gcloud"
	if _, err := exec.LookPath(bin); err != nil {
		bin = "gsutil"
	}
	path, err := exec.LookPath(bin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gcloud/gsutil not found")
		os.Exit(127)
	}
	cmd := exec.Command(path, os.Args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), "STORAGE_EMULATOR_HOST="+host)
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
