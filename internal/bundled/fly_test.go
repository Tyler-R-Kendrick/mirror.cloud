package bundled_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestFlyBundleBehaves asks the question loading a bundle does not: whether the
// rules mean what they say. `require` is where meaning inverts silently -- a
// whole bundle written as though `cond` were the trigger rather than the
// precondition validates perfectly and faults every request.
//
// It also pins the members the document unwrapped. The pack answered
// `{app: ...}` and `{machine: ...}`; the document declares the body to be the
// App or the Machine, so a client reads `name` where it used to read
// `app.name`.
func TestFlyBundleBehaves(t *testing.T) {
	pack, err := bundled.New("fly.machines", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) (*spi.Response, error) {
		return pack.Invoke(ctx, &spi.Request{ServiceID: "fly.machines", Operation: op, Identity: id, Input: in})
	}
	ok := func(t *testing.T, op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := call(op, in)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}
	fault := func(t *testing.T, err error, code string, status int) {
		t.Helper()
		f, isFault := err.(*spi.Fault)
		if !isFault {
			t.Fatalf("want %s, got %v", code, err)
		}
		if f.Code != code || f.HTTPStatus != status {
			t.Fatalf("want %s/%d, got %s/%d", code, status, f.Code, f.HTTPStatus)
		}
	}
	cfg := map[string]any{"image": "flyio/hello:1"}

	if out := ok(t, "AppsList", map[string]any{"org_slug": "personal"}); len(out["apps"].([]any)) != 0 {
		t.Fatalf("fresh account lists %v", out["apps"])
	}

	made := ok(t, "AppsCreate", map[string]any{"name": "app-one", "org_slug": "personal"})
	// The create answers only an id and a timestamp, which is all the document
	// declares on it -- the app itself comes from AppsShow.
	appID, _ := made["id"].(string)
	if appID == "" || made["created_at"] == nil {
		t.Fatalf("create answered %v", made)
	}
	if made["app"] != nil {
		t.Fatalf("the create is still wrapped: %v", made)
	}

	if _, err := call("AppsCreate", map[string]any{"name": "app-one", "org_slug": "personal"}); err == nil {
		t.Fatal("a duplicate app name was accepted")
	} else {
		fault(t, err, "taken", 422)
	}
	// The document does not mark `name` required, so only the bundle's own rule
	// stands between a nameless create and an app stored under the empty key.
	if _, err := call("AppsCreate", map[string]any{"org_slug": "personal"}); err == nil {
		t.Fatal("a nameless create was accepted")
	} else {
		fault(t, err, "invalid", 400)
	}

	shown := ok(t, "AppsShow", map[string]any{"app_name": "app-one"})
	if shown["name"] != "app-one" || shown["status"] != "pending" || shown["id"] != appID {
		t.Fatalf("show answered %v", shown)
	}
	if org, _ := shown["organization"].(map[string]any); org["slug"] != "personal" {
		t.Fatalf("organization = %v", shown["organization"])
	}
	if _, err := call("AppsShow", map[string]any{"app_name": "absent"}); err == nil {
		t.Fatal("a missing app was found")
	} else {
		fault(t, err, "not_found", 404)
	}

	listed := ok(t, "AppsList", map[string]any{"org_slug": "personal"})
	if apps, _ := listed["apps"].([]any); len(apps) != 1 || fmt.Sprint(listed["total_apps"]) != "1" {
		t.Fatalf("list answered %v", listed)
	}

	// A machine cannot be created under an app that does not exist, and that
	// check runs before the image check.
	if _, err := call("MachinesCreate", map[string]any{"app_name": "absent", "config": cfg}); err == nil {
		t.Fatal("a machine was created under a missing app")
	} else {
		fault(t, err, "not_found", 404)
	}
	if _, err := call("MachinesCreate", map[string]any{"app_name": "app-one"}); err == nil {
		t.Fatal("a machine with no image was accepted")
	} else {
		fault(t, err, "invalid", 400)
	}

	machine := ok(t, "MachinesCreate", map[string]any{
		"app_name": "app-one", "config": cfg, "region": "iad"})
	mid := fmt.Sprint(machine["id"])
	if mid == "" || machine["state"] != "created" || machine["region"] != "iad" {
		t.Fatalf("machine create answered %v", machine)
	}
	// An unnamed machine takes its own id as its name, as the pack had it.
	if machine["name"] != mid {
		t.Fatalf("machine name = %v, want the id", machine["name"])
	}
	if c, _ := machine["config"].(map[string]any); c["image"] != "flyio/hello:1" {
		t.Fatalf("config = %v", machine["config"])
	}

	got := ok(t, "MachinesShow", map[string]any{"app_name": "app-one", "machine_id": mid})
	if got["id"] != machine["id"] || got["state"] != "created" {
		t.Fatalf("machine show answered %v", got)
	}
	if _, err := call("MachinesShow", map[string]any{"app_name": "app-one", "machine_id": "deadbeefdeadbeef"}); err == nil {
		t.Fatal("a missing machine was found")
	} else {
		fault(t, err, "not_found", 404)
	}

	machines := ok(t, "MachinesList", map[string]any{"app_name": "app-one"})
	if items, _ := machines["_list"].([]any); len(items) != 1 {
		t.Fatalf("machine list answered %v", machines)
	}
	// A list is scoped to its app by the record, not by the collection.
	if _, err := call("MachinesList", map[string]any{"app_name": "absent"}); err == nil {
		t.Fatal("machines were listed for a missing app")
	} else {
		fault(t, err, "not_found", 404)
	}

	if out := ok(t, "MachinesDelete", map[string]any{"app_name": "app-one", "machine_id": mid}); len(out) != 0 {
		t.Fatalf("a bodyless delete answered %v", out)
	}
	if _, err := call("MachinesDelete", map[string]any{"app_name": "app-one", "machine_id": mid}); err == nil {
		t.Fatal("deleting twice succeeded")
	} else {
		fault(t, err, "not_found", 404)
	}

	if out := ok(t, "AppsDelete", map[string]any{"app_name": "app-one"}); len(out) != 0 {
		t.Fatalf("a bodyless delete answered %v", out)
	}
	if _, err := call("AppsDelete", map[string]any{"app_name": "app-one"}); err == nil {
		t.Fatal("deleting twice succeeded")
	} else {
		fault(t, err, "not_found", 404)
	}
	// The name is free again, which is the assertion a delete that forgot the
	// record would fail.
	ok(t, "AppsCreate", map[string]any{"name": "app-one", "org_slug": "personal"})
}
