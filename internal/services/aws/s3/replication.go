package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

type replicaRef struct {
	Account string `json:"account"`
	Region  string `json:"region"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
}

func (p *Pack) replicateObject(ctx context.Context, req *spi.Request, bucket, key string, body []byte, meta map[string]any) string {
	rules := p.replicationRules(ctx, req, bucket)
	tags := requestTags(req)
	if len(tags) > 0 {
		raw, _ := json.Marshal(tagSet(tags))
		_ = p.col(req, "tags").Put(ctx, bucket+"/"+key, raw)
	}
	matched, copied := false, false
	for _, rule := range rules {
		if !ruleMatches(rule, key, tags) {
			continue
		}
		matched = true
		ref, storageClass := p.replicationDestination(ctx, req, rule, key)
		if ref.Bucket == "" || ref.Bucket == bucket && ref.Account == req.Identity.Account && ref.Region == req.Identity.Region {
			continue
		}
		scope := p.deps.Store.Scope(ref.Account, ref.Region)
		if _, ok, _ := scope.Collection("buckets").Get(ctx, ref.Bucket); !ok {
			continue
		}
		info, err := p.deps.Blobs.Put(ctx, scopedBlobKey(ref.Account, ref.Region, ref.Bucket, key), bytes.NewReader(body))
		if err != nil {
			continue
		}
		dstMeta := cloneMap(meta)
		dstMeta["etag"], dstMeta["md5"], dstMeta["size"] = `"`+info.MD5+`"`, info.MD5, info.Size
		dstMeta["replicationStatus"] = "REPLICA"
		if storageClass != "" {
			dstMeta["storageClass"] = storageClass
		}
		raw, _ := json.Marshal(dstMeta)
		_ = scope.Collection("objects").Put(ctx, ref.Bucket+"/"+key, raw)
		if len(tags) > 0 {
			tagRaw, _ := json.Marshal(tagSet(tags))
			_ = scope.Collection("tags").Put(ctx, ref.Bucket+"/"+key, tagRaw)
		}
		p.rememberReplica(ctx, req, bucket, key, ref)
		copied = true
	}
	if copied {
		return "COMPLETED"
	}
	if matched {
		return "FAILED"
	}
	return ""
}

func (p *Pack) replicateDeleteMarker(ctx context.Context, req *spi.Request, bucket, key string, sourceMeta []byte) string {
	matched, copied := false, false
	tags := p.storedTags(ctx, req, bucket, key)
	for _, rule := range p.replicationRules(ctx, req, bucket) {
		deleteCfg := asMap(rule["DeleteMarkerReplication"])
		if !strings.EqualFold(str(deleteCfg["Status"]), "Enabled") || !ruleMatches(rule, key, tags) {
			continue
		}
		matched = true
		ref, _ := p.replicationDestination(ctx, req, rule, key)
		scope := p.deps.Store.Scope(ref.Account, ref.Region)
		if ref.Bucket == "" {
			continue
		}
		if _, ok, _ := scope.Collection("buckets").Get(ctx, ref.Bucket); !ok {
			continue
		}
		var meta map[string]any
		_ = json.Unmarshal(sourceMeta, &meta)
		meta["replicationStatus"] = "REPLICA"
		raw, _ := json.Marshal(meta)
		_ = scope.Collection("objects").Put(ctx, ref.Bucket+"/"+key, raw)
		copied = true
	}
	if copied {
		return "COMPLETED"
	}
	if matched {
		return "FAILED"
	}
	return ""
}

func (p *Pack) storedTags(ctx context.Context, req *spi.Request, bucket, key string) map[string]string {
	raw, ok, _ := p.col(req, "tags").Get(ctx, bucket+"/"+key)
	if !ok {
		return nil
	}
	var values []any
	_ = json.Unmarshal(raw, &values)
	tags := make(map[string]string, len(values))
	for _, value := range values {
		tag := asMap(value)
		tags[str(tag["Key"])] = str(tag["Value"])
	}
	return tags
}

func (p *Pack) replicationRules(ctx context.Context, req *spi.Request, bucket string) []map[string]any {
	raw, ok, _ := p.col(req, "bktcfg").Get(ctx, bucket+"/replication")
	if !ok {
		return nil
	}
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	if nested := asMap(doc["ReplicationConfiguration"]); len(nested) > 0 {
		doc = nested
	}
	var values []any
	if v := asSlice(doc["Rules"]); len(v) > 0 {
		values = v
	} else if v := asSlice(doc["Rule"]); len(v) > 0 {
		values = v
	} else if v := asMap(doc["Rule"]); len(v) > 0 {
		values = []any{v}
	}
	rules := make([]map[string]any, 0, len(values))
	for _, value := range values {
		rule := asMap(value)
		if !strings.EqualFold(str(rule["Status"]), "Disabled") {
			rules = append(rules, rule)
		}
	}
	return rules
}

func ruleMatches(rule map[string]any, key string, tags map[string]string) bool {
	prefix := str(rule["Prefix"])
	filter := asMap(rule["Filter"])
	if v := str(filter["Prefix"]); v != "" {
		prefix = v
	}
	and := asMap(filter["And"])
	if v := str(and["Prefix"]); v != "" {
		prefix = v
	}
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	want := map[string]string{}
	for _, source := range []map[string]any{filter, and} {
		if tag := asMap(source["Tag"]); len(tag) > 0 {
			want[str(tag["Key"])] = str(tag["Value"])
		}
		for _, value := range asSlice(source["Tags"]) {
			tag := asMap(value)
			want[str(tag["Key"])] = str(tag["Value"])
		}
	}
	for k, v := range want {
		if k == "" || tags[k] != v {
			return false
		}
	}
	return true
}

func (p *Pack) replicationDestination(ctx context.Context, req *spi.Request, rule map[string]any, key string) (replicaRef, string) {
	dst := asMap(rule["Destination"])
	bucket := str(dst["Bucket"])
	if i := strings.LastIndex(bucket, ":::"); i >= 0 {
		bucket = bucket[i+3:]
	}
	account, region := str(dst["Account"]), str(dst["Region"])
	if account == "" {
		account = req.Identity.Account
	}
	if region == "" {
		region = req.Identity.Region
	}
	if raw, ok, _ := p.deps.Store.Scope("_mirror", "global").Collection("s3buckets").Get(ctx, bucket); ok {
		var location map[string]any
		_ = json.Unmarshal(raw, &location)
		if str(dst["Account"]) == "" {
			account = str(location["account"])
		}
		if str(dst["Region"]) == "" {
			region = str(location["region"])
		}
	}
	return replicaRef{Account: account, Region: region, Bucket: bucket, Key: key}, str(dst["StorageClass"])
}

func requestTags(req *spi.Request) map[string]string {
	tagging := str(req.Input["Tagging"])
	if tagging == "" && req.HTTP != nil {
		tagging = req.HTTP.Header.Get("x-amz-tagging")
	}
	values, _ := url.ParseQuery(tagging)
	tags := make(map[string]string, len(values))
	for key := range values {
		tags[key] = values.Get(key)
	}
	return tags
}

func tagSet(tags map[string]string) []any {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, key := range keys {
		value := tags[key]
		out = append(out, map[string]any{"Key": key, "Value": value})
	}
	return out
}

func (p *Pack) rememberReplica(ctx context.Context, req *spi.Request, bucket, key string, ref replicaRef) {
	col := p.col(req, "replicas")
	raw, _, _ := col.Get(ctx, bucket+"/"+key)
	var refs []replicaRef
	_ = json.Unmarshal(raw, &refs)
	for _, existing := range refs {
		if existing == ref {
			return
		}
	}
	refs = append(refs, ref)
	raw, _ = json.Marshal(refs)
	_ = col.Put(ctx, bucket+"/"+key, raw)
}

func (p *Pack) syncReplicaTags(ctx context.Context, req *spi.Request, bucket, key string, tags []byte) {
	raw, ok, _ := p.col(req, "replicas").Get(ctx, bucket+"/"+key)
	if !ok {
		return
	}
	var refs []replicaRef
	_ = json.Unmarshal(raw, &refs)
	for _, ref := range refs {
		_ = p.deps.Store.Scope(ref.Account, ref.Region).Collection("tags").Put(ctx, ref.Bucket+"/"+ref.Key, tags)
	}
}

func (p *Pack) syncReplicaObjectLock(ctx context.Context, req *spi.Request, bucket, key, kind string, value []byte) {
	raw, ok, _ := p.col(req, "replicas").Get(ctx, bucket+"/"+key)
	if !ok {
		return
	}
	var refs []replicaRef
	_ = json.Unmarshal(raw, &refs)
	for _, ref := range refs {
		_ = p.deps.Store.Scope(ref.Account, ref.Region).Collection("objlock").Put(ctx, ref.Bucket+"/"+ref.Key+"/"+kind, value)
	}
}

func setReplicationHeaders(headers http.Header, meta map[string]any) {
	if status := str(meta["replicationStatus"]); status != "" {
		headers.Set("x-amz-replication-status", status)
	}
	if storageClass := str(meta["storageClass"]); storageClass != "" {
		headers.Set("x-amz-storage-class", storageClass)
	}
}

func scopedBlobKey(account, region, bucket, key string) string {
	return account + "/" + region + "/" + bucket + "/" + key
}

func cloneMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
