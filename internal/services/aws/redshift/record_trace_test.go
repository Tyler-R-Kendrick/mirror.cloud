package redshift

import (
	"context"
	"os"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/equivalence"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestRecordedTraceMatchesPack(t *testing.T) {
	f, err := equivalence.LoadFile(os.DirFS("../../../equivalence"), "traces/aws.redshift.json")
	if err != nil {
		t.Fatal(err)
	}
	trace := f.Trace()
	trace.Model = specboot.Bundle().ServiceByID("aws.redshift")
	if trace.Model == nil {
		t.Fatal("aws.redshift not in served model")
	}
	diffs, err := equivalence.Replay(context.Background(), New(spitest.Deps(t)), trace)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range diffs {
		t.Errorf("pack diverges from its recording: %s", d)
	}
}
