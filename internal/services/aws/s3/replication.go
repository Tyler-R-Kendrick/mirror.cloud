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
	Version string `json:"version,omitempty"`
}

func (p *Pack) replicateObject(ctx context.Context, req *spi.Request, bucket, key string, body []byte, meta map[string]any, tags map[string]string) string {
	rules := p.replicationRules(ctx, req, bucket)
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
		blob := scopedBlobKey(ref.Account, ref.Region, ref.Bucket, key)
		version := str(meta["versionId"])
		if version != "" {
			if _, err := p.deps.Blobs.Put(ctx, blob+"@"+version, bytes.NewReader(body)); err != nil {
				continue
			}
		}
		info, err := p.deps.Blobs.Put(ctx, blob, bytes.NewReader(body))
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
		if version != "" {
			_ = scope.Collection("versions").Put(ctx, ref.Bucket+"/"+key+"/"+version, raw)
			ref.Version = version
		}
		tagKeys := []string{objectTagKey(ref.Bucket, key, "")}
		if version != "" {
			tagKeys = append(tagKeys, objectTagKey(ref.Bucket, key, version))
		}
		if len(tags) > 0 {
			tagRaw, _ := json.Marshal(tagSet(tags))
			for _, tagKey := range tagKeys {
				_ = scope.Collection("tags").Put(ctx, tagKey, tagRaw)
			}
		} else {
			for _, tagKey := range tagKeys {
				_ = scope.Collection("tags").Delete(ctx, tagKey)
			}
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
	tags := p.storedTags(ctx, req, bucket, key, "")
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
		if version := str(meta["versionId"]); version != "" {
			_ = scope.Collection("versions").Put(ctx, ref.Bucket+"/"+key+"/"+version, raw)
		}
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

func (p *Pack) storedTags(ctx context.Context, req *spi.Request, bucket, key, version string) map[string]string {
	raw, ok, _ := p.col(req, "tags").Get(ctx, objectTagKey(bucket, key, version))
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

func objectTagKey(bucket, key, version string) string {
	tagKey := bucket + "/" + key
	if version != "" {
		tagKey += "/" + version
	}
	return tagKey
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

func requestTags(req *spi.Request) (map[string]string, error) {
	tagging := str(req.Input["Tagging"])
	if tagging == "" && req.HTTP != nil {
		tagging = req.HTTP.Header.Get("x-amz-tagging")
	}
	values, err := url.ParseQuery(tagging)
	if err != nil {
		return nil, invalidTaggingHeader(tagging)
	}
	tags := make(map[string]string, len(values))
	tagSet := make([]any, 0, len(values))
	for key, value := range values {
		if len(value) != 1 {
			return nil, invalidTaggingHeader(tagging)
		}
		tags[key] = value[0]
		tagSet = append(tagSet, map[string]any{"Key": key, "Value": value[0]})
	}
	if err := validateTagSet(tagSet, 10, "object"); err != nil {
		return nil, invalidTaggingHeader(tagging)
	}
	return tags, nil
}

func invalidTaggingHeader(value string) error {
	return &spi.Fault{Code: "InvalidArgument", Message: "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-tagging", "ArgumentValue": value}}
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

func (p *Pack) syncReplicaTags(ctx context.Context, req *spi.Request, bucket, key, version string, tags []byte) {
	raw, ok, _ := p.col(req, "replicas").Get(ctx, bucket+"/"+key)
	if !ok {
		return
	}
	var refs []replicaRef
	_ = json.Unmarshal(raw, &refs)
	for _, ref := range refs {
		if ref.Version != "" && ref.Version != version {
			continue
		}
		col := p.deps.Store.Scope(ref.Account, ref.Region).Collection("tags")
		_ = col.Put(ctx, objectTagKey(ref.Bucket, ref.Key, ""), tags)
		if version != "" {
			_ = col.Put(ctx, objectTagKey(ref.Bucket, ref.Key, version), tags)
		}
	}
}

func (p *Pack) syncReplicaObjectLock(ctx context.Context, req *spi.Request, bucket, key, version, kind string, value []byte) {
	raw, ok, _ := p.col(req, "replicas").Get(ctx, bucket+"/"+key)
	if !ok {
		return
	}
	var refs []replicaRef
	_ = json.Unmarshal(raw, &refs)
	for _, ref := range refs {
		if ref.Version != "" && ref.Version != version {
			continue
		}
		_ = p.deps.Store.Scope(ref.Account, ref.Region).Collection("objlock").Put(ctx, objectLockKey(ref.Bucket, ref.Key, version, kind), value)
	}
}

func setReplicationHeaders(headers http.Header, meta map[string]any) {
	if status := str(meta["replicationStatus"]); status != "" {
		headers.Set("x-amz-replication-status", status)
	}
	if storageClass := str(meta["storageClass"]); storageClass != "" && storageClass != "STANDARD" {
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
