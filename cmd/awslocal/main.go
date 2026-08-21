// Command awslocal wraps the AWS CLI against the local emulator.
package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:4566"
	}
	aws, err := exec.LookPath("aws")
	if err != nil {
		fmt.Fprintln(os.Stderr, "aws CLI not found; install AWS CLI v2")
		os.Exit(127)
	}
	args := append([]string{"--endpoint-url", endpoint}, os.Args[1:]...)
	cmd := exec.Command(aws, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = os.Environ()
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		cmd.Env = append(cmd.Env,
			"AWS_ACCESS_KEY_ID=test",
			"AWS_SECRET_ACCESS_KEY=test",
			"AWS_DEFAULT_REGION=us-east-1",
			"AWS_EC2_METADATA_DISABLED=true",
		)
	}
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
