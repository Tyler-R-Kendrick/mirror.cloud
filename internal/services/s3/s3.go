// Package s3 is the emulate-tier S3 behavior pack.
package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.s3", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements spi.BehaviorPack for S3.
type Pack struct {
	deps spi.Deps
	mu   sync.Mutex
	mpu  map[string]*mpu
}

type mpu struct {
	bucket, key, uploadID string
	parts                 map[int][]byte
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d, mpu: map[string]*mpu{}} }

func (p *Pack) ServiceID() string { return "aws.s3" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }

func (p *Pack) Operations() []string {
	return []string{
		"CreateBucket", "DeleteBucket", "HeadBucket", "ListBuckets", "GetBucketLocation",
		"GetBucketVersioning", "PutBucketVersioning", "GetBucketTagging", "PutBucketTagging",
		"GetBucketNotificationConfiguration", "PutBucketNotificationConfiguration",
		"GetBucketAcl", "GetBucketPolicy", "GetBucketCors", "GetBucketWebsite",
		"GetBucketLogging", "GetBucketLifecycleConfiguration", "GetBucketReplication",
		"GetBucketEncryption", "GetBucketObjectLockConfiguration", "GetBucketRequestPayment",
		"GetBucketAccelerateConfiguration",
		"PutObject", "GetObject", "HeadObject", "DeleteObject", "DeleteObjects", "CopyObject",
		"ListObjects", "ListObjectsV2", "ListObjectVersions",
		"CreateMultipartUpload", "UploadPart", "UploadPartCopy", "CompleteMultipartUpload",
		"AbortMultipartUpload", "ListParts", "ListMultipartUploads",
		"GetObjectTagging", "PutObjectTagging",
	}
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil && req.Operation != "" {
		req.Operation = p.route(req)
	}
	switch req.Operation {
	case "CreateBucket":
		return p.createBucket(ctx, req)
	case "DeleteBucket":
		return p.deleteBucket(ctx, req)
	case "HeadBucket":
		return p.headBucket(ctx, req)
	case "ListBuckets":
		return p.listBuckets(ctx, req)
	case "GetBucketLocation":
		return p.getBucketLocation(ctx, req)
	case "PutObject":
		return p.putObject(ctx, req)
	case "GetObject":
		return p.getObject(ctx, req)
	case "HeadObject":
		return p.headObject(ctx, req)
	case "DeleteObject":
		return p.deleteObject(ctx, req)
	case "ListObjects", "ListObjectsV2":
		return p.listObjects(ctx, req)
	case "CreateMultipartUpload":
		return p.createMPU(ctx, req)
	case "UploadPart":
		return p.uploadPart(ctx, req)
	case "CompleteMultipartUpload":
		return p.completeMPU(ctx, req)
	case "AbortMultipartUpload":
		return p.abortMPU(ctx, req)
	case "CopyObject":
		return p.copyObject(ctx, req)
	case "DeleteObjects":
		return p.deleteObjects(ctx, req)
	case "PutBucketVersioning", "GetBucketVersioning":
		return p.versioning(ctx, req)
	case "GetBucketAcl", "GetBucketPolicy", "GetBucketCors", "GetBucketWebsite",
		"GetBucketLogging", "GetBucketLifecycleConfiguration", "GetBucketReplication",
		"GetBucketEncryption", "GetBucketObjectLockConfiguration", "GetBucketRequestPayment",
		"GetBucketAccelerateConfiguration", "GetBucketTagging", "GetBucketNotificationConfiguration",
		"GetObjectTagging", "ListObjectVersions", "ListParts", "ListMultipartUploads",
		"PutBucketTagging", "PutBucketNotificationConfiguration", "PutObjectTagging",
		"UploadPartCopy":
		return p.emptyOK(req)
	default:
		return nil, spi.NotImplemented("aws.s3", req.Operation, "emulate")
	}
}

func (p *Pack) route(req *spi.Request) string {
	r := req.HTTP
	path := strings.TrimPrefix(r.URL.Path, "/")
	q := r.URL.Query()
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	bucket, key := "", ""
	if strings.Contains(host, ".s3.") {
		bucket = strings.Split(host, ".s3.")[0]
		key = path
	} else {
		parts := strings.SplitN(path, "/", 2)
		if len(parts) > 0 {
			bucket = parts[0]
		}
		if len(parts) > 1 {
			key = parts[1]
		}
	}
	req.Input["Bucket"] = bucket
	if key != "" {
		req.Input["Key"] = key
	}
	if r.Method == http.MethodGet && bucket == "" {
		return "ListBuckets"
	}
	if r.Method == http.MethodPut && key == "" && q.Get("versioning") != "" {
		return "PutBucketVersioning"
	}
	if r.Method == http.MethodGet && key == "" && q.Get("location") != "" {
		return "GetBucketLocation"
	}
	if r.Method == http.MethodGet && key == "" && q.Get("versioning") != "" {
		return "GetBucketVersioning"
	}
	if r.Method == http.MethodGet && key == "" && q.Get("acl") != "" {
		return "GetBucketAcl"
	}
	if r.Method == http.MethodHead && key == "" {
		return "HeadBucket"
	}
	if r.Method == http.MethodPut && key == "" {
		return "CreateBucket"
	}
	if r.Method == http.MethodDelete && key == "" {
		return "DeleteBucket"
	}
	if r.Method == http.MethodPost && q.Get("delete") != "" {
		return "DeleteObjects"
	}
	if r.Method == http.MethodPost && q.Get("uploads") != "" {
		return "CreateMultipartUpload"
	}
	if r.Method == http.MethodPut && q.Get("partNumber") != "" {
		return "UploadPart"
	}
	if r.Method == http.MethodPost && q.Get("uploadId") != "" {
		return "CompleteMultipartUpload"
	}
	if r.Method == http.MethodDelete && q.Get("uploadId") != "" {
		return "AbortMultipartUpload"
	}
	if r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "" {
		return "CopyObject"
	}
	if r.Method == http.MethodPut && key != "" {
		return "PutObject"
	}
	if r.Method == http.MethodGet && key != "" {
		return "GetObject"
	}
	if r.Method == http.MethodHead && key != "" {
		return "HeadObject"
	}
	if r.Method == http.MethodDelete && key != "" {
		return "DeleteObject"
	}
	if r.Method == http.MethodGet && key == "" {
		if q.Get("list-type") == "2" || q.Get("list-type") == "" {
			return "ListObjectsV2"
		}
		return "ListObjects"
	}
	return req.Operation
}

func (p *Pack) col(req *spi.Request, name string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(name)
}

func (p *Pack) createBucket(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if b == "" {
		return nil, &spi.Fault{Code: "InvalidBucketName", HTTPStatus: 400, Fault: "client"}
	}
	meta, _ := json.Marshal(map[string]any{"name": b, "region": req.Identity.Region})
	_ = p.col(req, "buckets").Put(ctx, b, meta)
	h := http.Header{}
	h.Set("Location", "/"+b)
	return &spi.Response{Status: 200, Headers: h, Output: map[string]any{}}, nil
}

func (p *Pack) deleteBucket(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	_ = p.col(req, "buckets").Delete(ctx, b)
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) headBucket(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireBucket(ctx, req, str(req.Input["Bucket"])); err != nil {
		return nil, err
	}
	return &spi.Response{Status: 200}, nil
}

func (p *Pack) listBuckets(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "buckets").List(ctx, "", "", 0)
	var buckets []any
	for _, kv := range kvs {
		buckets = append(buckets, map[string]any{"Name": kv.Key, "CreationDate": p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")})
	}
	return &spi.Response{Output: map[string]any{"Buckets": buckets, "Owner": map[string]any{"ID": req.Identity.Account, "DisplayName": "mirror"}}}, nil
}

func (p *Pack) getBucketLocation(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireBucket(ctx, req, str(req.Input["Bucket"])); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"LocationConstraint": req.Identity.Region}}, nil
}

func (p *Pack) putObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	info, err := p.deps.Blobs.Put(ctx, blobKey(req, b, key), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	etag := `"` + info.MD5 + `"`
	meta, _ := json.Marshal(map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5})
	_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
	h := http.Header{}
	h.Set("ETag", etag)
	if req.HTTP != nil {
		if c := req.HTTP.Header.Get("x-amz-checksum-crc32"); c != "" {
			h.Set("x-amz-checksum-crc32", c)
		}
	}
	p.notify(ctx, req, b, key, "ObjectCreated:Put")
	return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"ETag": etag}}, nil
}

func (p *Pack) getObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	rc, info, err := p.deps.Blobs.Get(ctx, blobKey(req, b, key))
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	h := http.Header{}
	etag := `"` + info.MD5 + `"`
	h.Set("ETag", etag)
	h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	h.Set("Last-Modified", p.deps.Clock.Now().UTC().Format(http.TimeFormat))
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if req.HTTP != nil {
		if rng := req.HTTP.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			data = applyRange(data, rng)
			h.Set("Content-Length", strconv.Itoa(len(data)))
			return &spi.Response{Status: 206, Headers: h, Stream: io.NopCloser(bytes.NewReader(data))}, nil
		}
	}
	return &spi.Response{Status: 200, Headers: h, Stream: io.NopCloser(bytes.NewReader(data))}, nil
}

func (p *Pack) headObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	info, err := p.deps.Blobs.Stat(ctx, blobKey(req, b, key))
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	h := http.Header{}
	h.Set("ETag", `"`+info.MD5+`"`)
	h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	h.Set("Last-Modified", p.deps.Clock.Now().UTC().Format(http.TimeFormat))
	return &spi.Response{Status: 200, Headers: h}, nil
}

func (p *Pack) deleteObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	_ = p.deps.Blobs.Delete(ctx, blobKey(req, b, key))
	_ = p.col(req, "objects").Delete(ctx, b+"/"+key)
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) deleteObjects(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{"Deleted": []any{}}}, nil
}

func (p *Pack) listObjects(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	prefix := str(req.Input["prefix"])
	if prefix == "" {
		prefix = str(req.Input["Prefix"])
	}
	delim := str(req.Input["delimiter"])
	if delim == "" {
		delim = str(req.Input["Delimiter"])
	}
	kvs, _, _ := p.col(req, "objects").List(ctx, b+"/"+prefix, "", 0)
	var contents []any
	common := map[string]bool{}
	for _, kv := range kvs {
		key := strings.TrimPrefix(kv.Key, b+"/")
		if delim != "" {
			rest := strings.TrimPrefix(key, prefix)
			if i := strings.Index(rest, delim); i >= 0 {
				common[prefix+rest[:i+len(delim)]] = true
				continue
			}
		}
		var meta map[string]any
		_ = json.Unmarshal(kv.Value, &meta)
		contents = append(contents, map[string]any{"Key": key, "Size": meta["size"], "ETag": meta["etag"]})
	}
	var prefixes []any
	for pfx := range common {
		prefixes = append(prefixes, map[string]any{"Prefix": pfx})
	}
	return &spi.Response{Output: map[string]any{
		"Name": b, "Prefix": prefix, "Delimiter": delim,
		"IsTruncated": false, "MaxKeys": 1000,
		"Contents": contents, "CommonPrefixes": prefixes, "KeyCount": len(contents),
	}}, nil
}

func (p *Pack) copyObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	src := ""
	if req.HTTP != nil {
		src = req.HTTP.Header.Get("x-amz-copy-source")
	}
	src = strings.TrimPrefix(src, "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: 400, Fault: "client"}
	}
	rc, _, err := p.deps.Blobs.Get(ctx, blobKey(req, parts[0], parts[1]))
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	defer rc.Close()
	req.Body = rc
	return p.putObject(ctx, req)
}

func (p *Pack) createMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := p.deps.Rand.Hex(16)
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	p.mu.Lock()
	p.mpu[id] = &mpu{bucket: b, key: key, uploadID: id, parts: map[int][]byte{}}
	p.mu.Unlock()
	return &spi.Response{Output: map[string]any{"Bucket": b, "Key": key, "UploadId": id}}, nil
}

func (p *Pack) uploadPart(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	pn := partNumber(req)
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	p.mu.Lock()
	u := p.mpu[id]
	if u != nil {
		u.parts[pn] = body
	}
	p.mu.Unlock()
	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	h := http.Header{}
	h.Set("ETag", etag)
	return &spi.Response{Headers: h, Output: map[string]any{"ETag": etag}}, nil
}

func (p *Pack) completeMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	p.mu.Lock()
	u := p.mpu[id]
	p.mu.Unlock()
	if u == nil {
		return nil, &spi.Fault{Code: "NoSuchUpload", HTTPStatus: 404, Fault: "client"}
	}
	var buf bytes.Buffer
	var md5s []byte
	for i := 1; i <= len(u.parts); i++ {
		part := u.parts[i]
		buf.Write(part)
		s := md5.Sum(part)
		md5s = append(md5s, s[:]...)
	}
	sum := md5.Sum(md5s)
	etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sum[:]), len(u.parts))
	req.Input["Bucket"], req.Input["Key"] = u.bucket, u.key
	req.Body = io.NopCloser(&buf)
	resp, err := p.putObject(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = http.Header{}
	}
	resp.Headers.Set("ETag", etag)
	resp.Output = map[string]any{"Bucket": u.bucket, "Key": u.key, "ETag": etag}
	return resp, nil
}

func (p *Pack) abortMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	p.mu.Lock()
	delete(p.mpu, id)
	p.mu.Unlock()
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) versioning(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{"Status": "Suspended"}}, nil
}

func (p *Pack) emptyOK(req *spi.Request) (*spi.Response, error) {
	return &spi.Response{Status: 200, Output: map[string]any{}}, nil
}

func (p *Pack) requireBucket(ctx context.Context, req *spi.Request, b string) error {
	_, ok, _ := p.col(req, "buckets").Get(ctx, b)
	if !ok {
		return &spi.Fault{Code: "NoSuchBucket", Message: "The specified bucket does not exist", HTTPStatus: 404, Fault: "client"}
	}
	return nil
}

func (p *Pack) notify(ctx context.Context, req *spi.Request, bucket, key, event string) {
	payload, _ := json.Marshal(map[string]any{
		"Records": []any{map[string]any{
			"eventName": event,
			"s3":        map[string]any{"bucket": map[string]any{"name": bucket}, "object": map[string]any{"key": key}},
		}},
	})
	_ = p.deps.Bus.Publish(ctx, "s3:"+bucket, payload)
}

func blobKey(req *spi.Request, b, k string) string {
	return req.Identity.Account + "/" + req.Identity.Region + "/" + b + "/" + k
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func mpuID(req *spi.Request) string {
	if s := str(req.Input["UploadId"]); s != "" {
		return s
	}
	if s := str(req.Input["uploadId"]); s != "" {
		return s
	}
	if req.HTTP != nil {
		return req.HTTP.URL.Query().Get("uploadId")
	}
	return ""
}

func partNumber(req *spi.Request) int {
	if n := asInt(req.Input["PartNumber"]); n > 0 {
		return n
	}
	if n := asInt(req.Input["partNumber"]); n > 0 {
		return n
	}
	if req.HTTP != nil {
		n, _ := strconv.Atoi(req.HTTP.URL.Query().Get("partNumber"))
		if n > 0 {
			return n
		}
	}
	return 1
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func applyRange(b []byte, rng string) []byte {
	rng = strings.TrimPrefix(rng, "bytes=")
	parts := strings.SplitN(rng, "-", 2)
	start, _ := strconv.Atoi(parts[0])
	end := len(b) - 1
	if len(parts) > 1 && parts[1] != "" {
		end, _ = strconv.Atoi(parts[1])
	}
	if start < 0 {
		start = 0
	}
	if end >= len(b) {
		end = len(b) - 1
	}
	if start > end {
		return nil
	}
	return b[start : end+1]
}
