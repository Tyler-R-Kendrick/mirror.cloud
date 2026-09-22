package cicd_test

import (
	"context"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd"
)

// ADV-CANCEL-RACE: Cancel overlapping Reconcile/Observe — terminal once, no resurrect.
func TestADV_CANCEL_RACE(t *testing.T) {
	ctx := context.Background()
	m := cicd.NewMemoryJobControl("adv-cancel")
	h, err := m.Submit(ctx, cicd.JobSpec{RunID: "r", SubmissionKey: "cancel-race"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = m.Cancel(ctx, h, "race")
	}()
	go func() {
		defer wg.Done()
		_, _ = m.Reconcile(ctx, h)
	}()
	wg.Wait()
	obs, err := m.Reconcile(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.Terminal {
		t.Fatalf("want terminal after cancel race: %+v", obs)
	}
	// Second cancel must not panic / rewrite to non-terminal.
	_ = m.Cancel(ctx, h, "again")
	again, err := m.Observe(ctx, h, "")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Terminal {
		t.Fatal("cancel resurrected job")
	}
}
