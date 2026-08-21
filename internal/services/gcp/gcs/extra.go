package gcs

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// leftoverOps are remaining Discovery operations served as control-plane KV.
func ExtraOps() []string { return extraOps() }

func extraOps() []string {
	return []string{
		"storage.anywhereCaches.disable",
		"storage.anywhereCaches.get",
		"storage.anywhereCaches.insert",
		"storage.anywhereCaches.list",
		"storage.anywhereCaches.pause",
		"storage.anywhereCaches.resume",
		"storage.anywhereCaches.update",
		"storage.bucketAccessControls.delete",
		"storage.bucketAccessControls.get",
		"storage.bucketAccessControls.insert",
		"storage.bucketAccessControls.list",
		"storage.bucketAccessControls.patch",
		"storage.bucketAccessControls.update",
		"storage.buckets.getIamPolicy",
		"storage.buckets.getStorageLayout",
		"storage.buckets.lockRetentionPolicy",
		"storage.buckets.operations.advanceRelocateBucket",
		"storage.buckets.operations.cancel",
		"storage.buckets.operations.get",
		"storage.buckets.operations.list",
		"storage.buckets.relocate",
		"storage.buckets.restore",
		"storage.buckets.setIamPolicy",
		"storage.buckets.testIamPermissions",
		"storage.buckets.update",
		"storage.channels.stop",
		"storage.defaultObjectAccessControls.delete",
		"storage.defaultObjectAccessControls.get",
		"storage.defaultObjectAccessControls.insert",
		"storage.defaultObjectAccessControls.list",
		"storage.defaultObjectAccessControls.patch",
		"storage.defaultObjectAccessControls.update",
		"storage.folders.delete",
		"storage.folders.deleteRecursive",
		"storage.folders.get",
		"storage.folders.insert",
		"storage.folders.list",
		"storage.folders.rename",
		"storage.managedFolders.delete",
		"storage.managedFolders.get",
		"storage.managedFolders.getIamPolicy",
		"storage.managedFolders.insert",
		"storage.managedFolders.list",
		"storage.managedFolders.setIamPolicy",
		"storage.managedFolders.testIamPermissions",
		"storage.managedFolders.update",
		"storage.notifications.delete",
		"storage.notifications.get",
		"storage.notifications.insert",
		"storage.notifications.list",
		"storage.objectAccessControls.delete",
		"storage.objectAccessControls.get",
		"storage.objectAccessControls.insert",
		"storage.objectAccessControls.list",
		"storage.objectAccessControls.patch",
		"storage.objectAccessControls.update",
		"storage.objects.bulkRestore",
		"storage.objects.getIamPolicy",
		"storage.objects.move",
		"storage.objects.restore",
		"storage.objects.setIamPolicy",
		"storage.objects.testIamPermissions",
		"storage.objects.update",
		"storage.projects.hmacKeys.create",
		"storage.projects.hmacKeys.delete",
		"storage.projects.hmacKeys.get",
		"storage.projects.hmacKeys.list",
		"storage.projects.hmacKeys.update",
		"storage.projects.serviceAccount.get",
		"storage.rapidCaches.disable",
		"storage.rapidCaches.get",
		"storage.rapidCaches.insert",
		"storage.rapidCaches.list",
		"storage.rapidCaches.update",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	id := str(req.Input["bucket"])
	if id == "" {
		id = str(req.Input["name"])
	}
	if id == "" {
		id = str(req.Input["object"])
	}
	c := p.col(req, "gcsl")
	switch {
	case extraWrite(op):
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "name": id}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = c.Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case extraDelete(op):
		if id != "" {
			_ = c.Delete(ctx, id)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case leftoverList(op):
		kvs, _, _ := c.List(ctx, "", "", 0)
		items := make([]any, 0, len(kvs))
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"items": items}}, nil
	default:
		if b, ok, _ := c.Get(ctx, id); ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			return &spi.Response{Output: rec}, nil
		}
		return &spi.Response{Output: map[string]any{}}, nil
	}
}

func extraWrite(op string) bool {
	for _, s := range []string{
		".insert", ".update", ".patch", ".create", ".setIamPolicy", ".lockRetentionPolicy",
		".relocate", ".restore", ".move", ".disable", ".pause", ".resume", ".stop", ".rename",
		".cancel", ".advanceRelocateBucket", ".bulkRestore",
	} {
		if strings.HasSuffix(op, s) {
			return true
		}
	}
	return false
}

func extraDelete(op string) bool {
	return strings.HasSuffix(op, ".delete") || strings.HasSuffix(op, ".deleteRecursive")
}

func leftoverList(op string) bool {
	return strings.HasSuffix(op, ".list") ||
		strings.HasSuffix(op, ".getIamPolicy") || strings.HasSuffix(op, ".getStorageLayout") ||
		strings.HasSuffix(op, ".testIamPermissions")
}
