// Package gcs is the emulate-tier Google Cloud Storage pack (cross-cloud proof).
package gcs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "gcp.storage", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements spi.BehaviorPack for GCS JSON API v1.
type Pack struct {
	deps spi.Deps
	mu   sync.Mutex
	sess map[string]*session
}

type session struct {
	bucket, name, contentType string
	buf                       []byte
	total                     int64
}

type bucketRec struct {
	Name           string `json:"name"`
	Metageneration string `json:"metageneration"`
	TimeCreated    string `json:"timeCreated"`
	Updated        string `json:"updated"`
	Location       string `json:"location"`
	StorageClass   string `json:"storageClass"`
}

type objectRec struct {
	Name           string `json:"name"`
	Bucket         string `json:"bucket"`
	Generation     string `json:"generation"`
	Metageneration string `json:"metageneration"`
	ContentType    string `json:"contentType"`
	Size           string `json:"size"`
	MD5            string `json:"md5Hash"`
	CRC32C         string `json:"crc32c,omitempty"`
	Etag           string `json:"etag"`
	TimeCreated    string `json:"timeCreated"`
	Updated        string `json:"updated"`
	StorageClass   string `json:"storageClass"`
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d, sess: map[string]*session{}} }

// ServiceID returns gcp.storage.
func (p *Pack) ServiceID() string { return "gcp.storage" }

// Tier returns emulate.
func (p *Pack) Tier() model.Tier { return model.TierEmulate }

// Operations lists emulate-tier GCS JSON API operations.
func (p *Pack) Operations() []string {
	return []string{
		"storage.buckets.insert", "storage.buckets.get", "storage.buckets.list",
		"storage.buckets.delete", "storage.buckets.patch",
		"storage.objects.insert", "storage.objects.get", "storage.objects.list",
		"storage.objects.delete", "storage.objects.copy", "storage.objects.rewrite",
		"storage.objects.compose", "storage.objects.patch",
	}
}

// Invoke dispatches one GCS JSON API operation.
func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	p.hydrate(req)
	path := str(req.Input["_path"])
	if req.HTTP != nil && req.HTTP.URL != nil {
		path = req.HTTP.URL.Path
	}
	if strings.Contains(path, "/batch") {
		return nil, spi.NotImplemented("gcp.storage", req.Operation, "emulate")
	}
	if req.HTTP != nil {
		req.Operation = p.route(req)
	}
	switch req.Operation {
	case "storage.buckets.insert":
		return p.bucketInsert(ctx, req)
	case "storage.buckets.get":
		return p.bucketGet(ctx, req)
	case "storage.buckets.list":
		return p.bucketList(ctx, req)
	case "storage.buckets.delete":
		return p.bucketDelete(ctx, req)
	case "storage.buckets.patch":
		return p.bucketPatch(ctx, req)
	case "storage.objects.insert":
		return p.objectInsert(ctx, req)
	case "storage.objects.get":
		return p.objectGet(ctx, req)
	case "storage.objects.list":
		return p.objectList(ctx, req)
	case "storage.objects.delete":
		return p.objectDelete(ctx, req)
	case "storage.objects.copy":
		return p.objectCopy(ctx, req)
	case "storage.objects.rewrite":
		return p.objectRewrite(ctx, req)
	case "storage.objects.compose":
		return p.objectCompose(ctx, req)
	case "storage.objects.patch":
		return p.objectPatch(ctx, req)
	default:
		return nil, spi.NotImplemented("gcp.storage", req.Operation, "emulate")
	}
}

func (p *Pack) route(req *spi.Request) string {
	r := req.HTTP
	path := r.URL.Path
	q := r.URL.Query()
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	if q.Get("name") != "" {
		req.Input["name"] = q.Get("name")
	}
	if q.Get("uploadType") != "" {
		req.Input["uploadType"] = q.Get("uploadType")
	}
	if q.Get("upload_id") != "" {
		req.Input["upload_id"] = q.Get("upload_id")
	}
	bkt, obj := parsePath(path)
	if bkt != "" && req.Input["bucket"] == nil {
		req.Input["bucket"] = bkt
	}
	if obj != "" && req.Input["object"] == nil {
		req.Input["object"] = obj
		if req.Input["name"] == nil {
			req.Input["name"] = obj
		}
	}
	if strings.Contains(path, "/upload/") && r.Method == http.MethodPut {
		return "storage.objects.insert"
	}
	if strings.Contains(path, "/rewriteTo/") {
		return "storage.objects.rewrite"
	}
	if strings.Contains(path, "/copyTo/") {
		return "storage.objects.copy"
	}
	if strings.HasSuffix(path, "/compose") {
		return "storage.objects.compose"
	}
	switch {
	case r.Method == http.MethodPost && strings.Contains(path, "/upload/"):
		return "storage.objects.insert"
	case r.Method == http.MethodPost && strings.Contains(path, "/o") && q.Get("uploadType") != "":
		return "storage.objects.insert"
	case r.Method == http.MethodPatch && obj != "":
		return "storage.objects.patch"
	case r.Method == http.MethodPatch && bkt != "":
		return "storage.buckets.patch"
	case r.Method == http.MethodDelete && obj != "":
		return "storage.objects.delete"
	case r.Method == http.MethodDelete:
		return "storage.buckets.delete"
	case r.Method == http.MethodGet && obj != "":
		return "storage.objects.get"
	case r.Method == http.MethodGet && strings.HasSuffix(strings.TrimSuffix(path, "/"), "/o"):
		return "storage.objects.list"
	case r.Method == http.MethodGet && bkt != "":
		return "storage.buckets.get"
	case r.Method == http.MethodGet && (strings.HasSuffix(path, "/b") || strings.HasSuffix(path, "/b/")):
		return "storage.buckets.list"
	case r.Method == http.MethodPost && bkt == "":
		return "storage.buckets.insert"
	case r.Method == http.MethodPost:
		return "storage.objects.insert"
	}
	return req.Operation
}

func (p *Pack) hydrate(req *spi.Request) {
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	path := str(req.Input["_path"])
	if path == "" && req.HTTP != nil && req.HTTP.URL != nil {
		path = req.HTTP.URL.Path
		req.Input["_path"] = path
	}
	if req.HTTP != nil && req.HTTP.URL != nil {
		q := req.HTTP.URL.Query()
		for _, k := range []string{
			"name", "uploadType", "upload_id", "prefix", "delimiter", "pageToken",
			"alt", "generation", "ifGenerationMatch", "ifMetagenerationMatch",
			"destinationBucket", "destinationObject", "rewriteToken", "maxResults",
		} {
			if str(req.Input[k]) == "" && q.Get(k) != "" {
				req.Input[k] = q.Get(k)
			}
		}
	}
	bkt, obj := parsePath(path)
	if bkt != "" && str(req.Input["bucket"]) == "" {
		req.Input["bucket"] = bkt
	}
	if obj != "" && str(req.Input["object"]) == "" {
		req.Input["object"] = obj
	}
	if obj != "" && str(req.Input["name"]) == "" && req.Operation != "storage.buckets.insert" {
		req.Input["name"] = obj
	}
	if db, dn := destFromPath(path); db != "" || dn != "" {
		if str(req.Input["destinationBucket"]) == "" {
			req.Input["destinationBucket"] = db
		}
		if str(req.Input["destinationObject"]) == "" {
			req.Input["destinationObject"] = dn
		}
	}
}

func destFromPath(path string) (bucket, object string) {
	for _, marker := range []string{"/copyTo/b/", "/rewriteTo/b/"} {
		i := strings.Index(path, marker)
		if i < 0 {
			continue
		}
		rest := path[i+len(marker):]
		parts := strings.SplitN(rest, "/o/", 2)
		if len(parts) != 2 {
			return rest, ""
		}
		obj, err := url.PathUnescape(parts[1])
		if err != nil {
			obj = parts[1]
		}
		return parts[0], obj
	}
	return "", ""
}

func parsePath(path string) (bucket, object string) {
	// /storage/v1/b/{bucket}/o/{object} or /upload/storage/v1/b/{bucket}/o
	s := path
	for _, pfx := range []string{"/upload/storage/v1/b/", "/storage/v1/b/", "/b/"} {
		if i := strings.Index(s, pfx); i >= 0 {
			s = s[i+len(pfx):]
			break
		}
	}
	s = strings.TrimPrefix(s, "/")
	if s == "" || s == "b" {
		return "", ""
	}
	parts := strings.SplitN(s, "/o/", 2)
	if len(parts) == 1 {
		bucket = strings.TrimSuffix(parts[0], "/o")
		bucket = strings.Trim(bucket, "/")
		return bucket, ""
	}
	bucket = strings.Trim(parts[0], "/")
	object, _ = url.PathUnescape(parts[1])
	if i := strings.IndexAny(object, "/?"); i >= 0 {
		// strip /copyTo /rewriteTo /compose
		if j := strings.Index(object, "/copyTo"); j >= 0 {
			object = object[:j]
		} else if j := strings.Index(object, "/rewriteTo"); j >= 0 {
			object = object[:j]
		} else if j := strings.Index(object, "/compose"); j >= 0 {
			object = object[:j]
		}
	}
	return bucket, object
}

func (p *Pack) col(req *spi.Request, name string) spi.Collection {
	acct := req.Identity.Account
	if acct == "" {
		acct = "000000000000"
	}
	reg := req.Identity.Region
	if reg == "" {
		reg = "us-east-1"
	}
	if req.Identity.Project != "" {
		acct = req.Identity.Project
	}
	return p.deps.Store.Scope(acct, reg).Collection(name)
}

func (p *Pack) bucketInsert(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["name"])
	if name == "" {
		return nil, fault("invalid", "bucket name required", 400)
	}
	now := p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05Z")
	rec := bucketRec{Name: name, Metageneration: "1", TimeCreated: now, Updated: now, Location: "US", StorageClass: "STANDARD"}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "buckets").Put(ctx, name, b)
	return &spi.Response{Status: 200, Output: p.bucketResource(req, rec)}, nil
}

func (p *Pack) bucketGet(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	rec, err := p.loadBucket(ctx, req, p.bucketName(req))
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: p.bucketResource(req, rec)}, nil
}

func (p *Pack) bucketList(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, err := p.col(req, "buckets").List(ctx, "", "", 0)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec bucketRec
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, p.bucketResource(req, rec))
	}
	return &spi.Response{Output: map[string]any{"kind": "storage#buckets", "items": items}}, nil
}

func (p *Pack) bucketDelete(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := p.bucketName(req)
	if _, err := p.loadBucket(ctx, req, name); err != nil {
		return nil, err
	}
	objs, _, _ := p.col(req, "objects").List(ctx, name+"/", "", 1)
	if len(objs) > 0 {
		return nil, fault("conflict", "bucket not empty", 409)
	}
	_ = p.col(req, "buckets").Delete(ctx, name)
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) bucketPatch(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	rec, err := p.loadBucket(ctx, req, p.bucketName(req))
	if err != nil {
		return nil, err
	}
	if loc := str(req.Input["location"]); loc != "" {
		rec.Location = loc
	}
	if sc := str(req.Input["storageClass"]); sc != "" {
		rec.StorageClass = sc
	}
	n, _ := strconv.ParseInt(rec.Metageneration, 10, 64)
	rec.Metageneration = strconv.FormatInt(n+1, 10)
	rec.Updated = p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05Z")
	b, _ := json.Marshal(rec)
	_ = p.col(req, "buckets").Put(ctx, rec.Name, b)
	return &spi.Response{Output: p.bucketResource(req, rec)}, nil
}

func (p *Pack) objectInsert(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket := p.bucketName(req)
	if _, err := p.loadBucket(ctx, req, bucket); err != nil {
		return nil, err
	}
	uploadType := str(req.Input["uploadType"])
	uploadID := str(req.Input["upload_id"])
	if uploadID != "" {
		return p.resumablePut(ctx, req, uploadID)
	}
	name := str(req.Input["name"])
	if name == "" {
		name = str(req.Input["object"])
	}
	if uploadType == "resumable" && (req.HTTP == nil || req.HTTP.Method == http.MethodPost) {
		id := p.deps.Rand.Hex(16)
		p.mu.Lock()
		p.sess[id] = &session{bucket: bucket, name: name, contentType: str(req.Input["contentType"])}
		p.mu.Unlock()
		h := http.Header{}
		loc := p.base(req) + "/upload/storage/v1/b/" + url.PathEscape(bucket) + "/o?uploadType=resumable&upload_id=" + id
		if name != "" {
			loc += "&name=" + url.QueryEscape(name)
		}
		h.Set("Location", loc)
		return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"upload_id": id}}, nil
	}
	body, name, ctype, err := p.readInsertBody(req, name)
	if err != nil {
		return nil, err
	}
	if uploadType == "" && !strings.Contains(str(req.Input["_path"]), "/upload/") {
		var meta map[string]any
		if json.Unmarshal(body, &meta) == nil && len(meta) > 0 {
			if n := str(meta["name"]); n != "" {
				name = n
			}
			if c := str(meta["contentType"]); c != "" {
				ctype = c
			}
			body = nil
		}
	}
	if name == "" {
		return nil, fault("invalid", "object name required", 400)
	}
	return p.storeObject(ctx, req, bucket, name, ctype, body)
}

func (p *Pack) resumablePut(ctx context.Context, req *spi.Request, id string) (*spi.Response, error) {
	p.mu.Lock()
	s := p.sess[id]
	p.mu.Unlock()
	if s == nil {
		return nil, fault("notFound", "unknown upload_id", 404)
	}
	chunk, _ := io.ReadAll(readerOf(req))
	cr := ""
	if req.HTTP != nil {
		cr = req.HTTP.Header.Get("Content-Range")
	}
	start, end, total, ok := parseContentRange(cr, int64(len(chunk)))
	if !ok {
		s.buf = append(s.buf, chunk...)
		if s.name == "" {
			s.name = str(req.Input["name"])
		}
		return p.storeObject(ctx, req, s.bucket, s.name, s.contentType, s.buf)
	}
	need := start + int64(len(chunk))
	if int64(len(s.buf)) < need {
		nb := make([]byte, need)
		copy(nb, s.buf)
		s.buf = nb
	}
	copy(s.buf[start:], chunk)
	if end+1 > int64(len(s.buf)) {
		s.buf = s.buf[:end+1]
	}
	s.total = total
	if s.name == "" {
		s.name = str(req.Input["name"])
	}
	if total >= 0 && end+1 >= total {
		return p.storeObject(ctx, req, s.bucket, s.name, s.contentType, s.buf[:total])
	}
	h := http.Header{}
	h.Set("Range", fmt.Sprintf("bytes=0-%d", len(s.buf)-1))
	return &spi.Response{Status: 308, Headers: h}, nil
}

func (p *Pack) objectGet(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket := p.bucketName(req)
	name := p.objectName(req)
	rec, err := p.loadObject(ctx, req, bucket, name)
	if err != nil {
		return nil, err
	}
	if err := p.checkPreconditions(req, rec); err != nil {
		return nil, err
	}
	alt := str(req.Input["alt"])
	if alt == "media" || (req.HTTP != nil && req.HTTP.URL.Query().Get("alt") == "media") {
		rc, info, err := p.deps.Blobs.Get(ctx, blobKey(req, bucket, name))
		if err != nil {
			return nil, fault("notFound", "object not found", 404)
		}
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		status := 200
		h := http.Header{}
		if req.HTTP != nil {
			if rng := req.HTTP.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
				orig := len(data)
				data = applyRange(data, rng)
				r := strings.TrimPrefix(rng, "bytes=")
				parts := strings.SplitN(r, "-", 2)
				start, _ := strconv.Atoi(parts[0])
				end := start + len(data) - 1
				if end < start {
					end = start
				}
				h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, orig))
				status = 206
			}
		}
		h.Set("Content-Type", rec.ContentType)
		if rec.ContentType == "" {
			h.Set("Content-Type", "application/octet-stream")
		}
		h.Set("Content-Length", strconv.Itoa(len(data)))
		h.Set("ETag", rec.Etag)
		h.Set("x-goog-generation", rec.Generation)
		_ = info
		return &spi.Response{Status: status, Headers: h, Stream: io.NopCloser(bytes.NewReader(data))}, nil
	}
	return &spi.Response{Output: p.objectResource(req, rec)}, nil
}

func (p *Pack) objectList(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket := p.bucketName(req)
	if _, err := p.loadBucket(ctx, req, bucket); err != nil {
		return nil, err
	}
	prefix := str(req.Input["prefix"])
	delim := str(req.Input["delimiter"])
	pageTok := str(req.Input["pageToken"])
	max := 1000
	if n := str(req.Input["maxResults"]); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			max = v
		}
	}
	kvs, _, err := p.col(req, "objects").List(ctx, bucket+"/"+prefix, pageTok, 0)
	if err != nil {
		return nil, err
	}
	items := []any{}
	prefixes := map[string]bool{}
	var next string
	for _, kv := range kvs {
		var rec objectRec
		_ = json.Unmarshal(kv.Value, &rec)
		rest := strings.TrimPrefix(rec.Name, prefix)
		if delim != "" {
			if i := strings.Index(rest, delim); i >= 0 {
				prefixes[prefix+rest[:i+len(delim)]] = true
				continue
			}
		}
		items = append(items, p.objectResource(req, rec))
		if len(items) >= max {
			next = kv.Key
			break
		}
	}
	out := map[string]any{"kind": "storage#objects", "items": items}
	if len(prefixes) > 0 {
		var keys []string
		for pfx := range prefixes {
			keys = append(keys, pfx)
		}
		sort.Strings(keys)
		ps := make([]any, len(keys))
		for i, pfx := range keys {
			ps[i] = pfx
		}
		out["prefixes"] = ps
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) objectDelete(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket, name := p.bucketName(req), p.objectName(req)
	if _, err := p.loadObject(ctx, req, bucket, name); err != nil {
		return nil, err
	}
	_ = p.col(req, "objects").Delete(ctx, bucket+"/"+name)
	_ = p.deps.Blobs.Delete(ctx, blobKey(req, bucket, name))
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) objectCopy(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	srcB, srcN := p.bucketName(req), p.objectName(req)
	dstB, dstN := dest(req)
	if dstB == "" {
		dstB = srcB
	}
	if dstN == "" {
		return nil, fault("invalid", "destination object required", 400)
	}
	rec, err := p.loadObject(ctx, req, srcB, srcN)
	if err != nil {
		return nil, err
	}
	rc, _, err := p.deps.Blobs.Get(ctx, blobKey(req, srcB, srcN))
	if err != nil {
		return nil, err
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	rec.Bucket, rec.Name = dstB, dstN
	return p.storeObject(ctx, req, dstB, dstN, rec.ContentType, data)
}

func (p *Pack) objectRewrite(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	resp, err := p.objectCopy(ctx, req)
	if err != nil {
		return nil, err
	}
	resp.Output = map[string]any{"kind": "storage#rewriteResponse", "done": true, "resource": resp.Output}
	return resp, nil
}

func (p *Pack) objectCompose(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket := p.bucketName(req)
	name := p.objectName(req)
	if name == "" {
		name = str(req.Input["destination"])
	}
	var srcs []any
	switch v := req.Input["sourceObjects"].(type) {
	case []any:
		srcs = v
	}
	var buf []byte
	for _, s := range srcs {
		m, _ := s.(map[string]any)
		n := str(m["name"])
		rc, _, err := p.deps.Blobs.Get(ctx, blobKey(req, bucket, n))
		if err != nil {
			return nil, fault("notFound", "source "+n, 404)
		}
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		buf = append(buf, b...)
	}
	return p.storeObject(ctx, req, bucket, name, "application/octet-stream", buf)
}

func (p *Pack) objectPatch(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket, name := p.bucketName(req), p.objectName(req)
	rec, err := p.loadObject(ctx, req, bucket, name)
	if err != nil {
		return nil, err
	}
	if err := p.checkPreconditions(req, rec); err != nil {
		return nil, err
	}
	if ct := str(req.Input["contentType"]); ct != "" {
		rec.ContentType = ct
	}
	n, _ := strconv.ParseInt(rec.Metageneration, 10, 64)
	rec.Metageneration = strconv.FormatInt(n+1, 10)
	rec.Updated = p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05Z")
	b, _ := json.Marshal(rec)
	_ = p.col(req, "objects").Put(ctx, bucket+"/"+name, b)
	return &spi.Response{Output: p.objectResource(req, rec)}, nil
}

func (p *Pack) storeObject(ctx context.Context, req *spi.Request, bucket, name, ctype string, body []byte) (*spi.Response, error) {
	if _, err := p.loadBucket(ctx, req, bucket); err != nil {
		return nil, err
	}
	if err := p.checkCreatePreconditions(ctx, req, bucket, name); err != nil {
		return nil, err
	}
	info, err := p.deps.Blobs.Put(ctx, blobKey(req, bucket, name), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	now := p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05Z")
	gen := strconv.FormatInt(int64(p.deps.Rand.Intn(1<<30))+1, 10)
	if existing, err := p.loadObject(ctx, req, bucket, name); err == nil {
		n, _ := strconv.ParseInt(existing.Generation, 10, 64)
		gen = strconv.FormatInt(n+1, 10)
	}
	rec := objectRec{
		Name: name, Bucket: bucket, Generation: gen, Metageneration: "1",
		ContentType: ctype, Size: strconv.FormatInt(info.Size, 10),
		MD5:  base64.StdEncoding.EncodeToString(mustDecodeHex(info.MD5)),
		Etag: info.MD5, TimeCreated: now, Updated: now, StorageClass: "STANDARD",
	}
	if rec.ContentType == "" {
		rec.ContentType = "application/octet-stream"
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "objects").Put(ctx, bucket+"/"+name, b)
	return &spi.Response{Status: 200, Output: p.objectResource(req, rec)}, nil
}

func (p *Pack) readInsertBody(req *spi.Request, name string) ([]byte, string, string, error) {
	ctype := str(req.Input["contentType"])
	r := readerOf(req)
	if req.HTTP != nil {
		ct := req.HTTP.Header.Get("Content-Type")
		if ctype == "" && ct != "" && !strings.HasPrefix(ct, "multipart/") {
			ctype = ct
		}
		if strings.HasPrefix(ct, "multipart/") {
			_, params, _ := mime.ParseMediaType(ct)
			mr := multipart.NewReader(r, params["boundary"])
			var meta map[string]any
			var body []byte
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					break
				}
				b, _ := io.ReadAll(part)
				if strings.Contains(part.Header.Get("Content-Type"), "json") && meta == nil {
					_ = json.Unmarshal(b, &meta)
				} else {
					body = b
				}
			}
			if meta != nil {
				if n := str(meta["name"]); n != "" {
					name = n
				}
				if c := str(meta["contentType"]); c != "" {
					ctype = c
				}
			}
			return body, name, ctype, nil
		}
	}
	b, _ := io.ReadAll(r)
	return b, name, ctype, nil
}

func (p *Pack) loadBucket(ctx context.Context, req *spi.Request, name string) (bucketRec, error) {
	b, ok, err := p.col(req, "buckets").Get(ctx, name)
	if err != nil {
		return bucketRec{}, err
	}
	if !ok {
		return bucketRec{}, fault("notFound", "bucket "+name+" not found", 404)
	}
	var rec bucketRec
	_ = json.Unmarshal(b, &rec)
	return rec, nil
}

func (p *Pack) loadObject(ctx context.Context, req *spi.Request, bucket, name string) (objectRec, error) {
	b, ok, err := p.col(req, "objects").Get(ctx, bucket+"/"+name)
	if err != nil {
		return objectRec{}, err
	}
	if !ok {
		return objectRec{}, fault("notFound", "object "+name+" not found", 404)
	}
	var rec objectRec
	_ = json.Unmarshal(b, &rec)
	return rec, nil
}

func (p *Pack) checkPreconditions(req *spi.Request, rec objectRec) error {
	if v := str(req.Input["ifGenerationMatch"]); v != "" && v != rec.Generation {
		return fault("conditionNotMet", "ifGenerationMatch", 412)
	}
	if v := str(req.Input["ifMetagenerationMatch"]); v != "" && v != rec.Metageneration {
		return fault("conditionNotMet", "ifMetagenerationMatch", 412)
	}
	if req.HTTP != nil {
		q := req.HTTP.URL.Query()
		if v := q.Get("ifGenerationMatch"); v != "" && v != rec.Generation {
			return fault("conditionNotMet", "ifGenerationMatch", 412)
		}
		if v := q.Get("ifMetagenerationMatch"); v != "" && v != rec.Metageneration {
			return fault("conditionNotMet", "ifMetagenerationMatch", 412)
		}
	}
	return nil
}

func (p *Pack) checkCreatePreconditions(ctx context.Context, req *spi.Request, bucket, name string) error {
	v := str(req.Input["ifGenerationMatch"])
	if req.HTTP != nil && v == "" {
		v = req.HTTP.URL.Query().Get("ifGenerationMatch")
	}
	if v == "" {
		return nil
	}
	existing, err := p.loadObject(ctx, req, bucket, name)
	exists := err == nil
	if v == "0" {
		if exists {
			return fault("conditionNotMet", "object exists", 412)
		}
		return nil
	}
	if !exists || existing.Generation != v {
		return fault("conditionNotMet", "ifGenerationMatch", 412)
	}
	return nil
}

func (p *Pack) bucketName(req *spi.Request) string {
	if v := str(req.Input["bucket"]); v != "" {
		return v
	}
	return str(req.Input["name"])
}

func (p *Pack) objectName(req *spi.Request) string {
	if v := str(req.Input["object"]); v != "" {
		return v
	}
	return str(req.Input["name"])
}

func (p *Pack) base(req *spi.Request) string {
	if req.HTTP != nil && req.HTTP.Host != "" {
		scheme := "http"
		if req.HTTP.TLS != nil {
			scheme = "https"
		}
		return scheme + "://" + req.HTTP.Host
	}
	return "http://127.0.0.1:4566"
}

func (p *Pack) bucketResource(req *spi.Request, rec bucketRec) map[string]any {
	base := p.base(req)
	return map[string]any{
		"kind": "storage#bucket", "id": rec.Name, "name": rec.Name,
		"selfLink":       base + "/storage/v1/b/" + rec.Name,
		"metageneration": rec.Metageneration, "timeCreated": rec.TimeCreated,
		"updated": rec.Updated, "location": rec.Location, "storageClass": rec.StorageClass,
	}
}

func (p *Pack) objectResource(req *spi.Request, rec objectRec) map[string]any {
	base := p.base(req)
	enc := url.PathEscape(rec.Name)
	return map[string]any{
		"kind": "storage#object", "id": rec.Bucket + "/" + rec.Name + "/" + rec.Generation,
		"selfLink":  base + "/storage/v1/b/" + rec.Bucket + "/o/" + enc,
		"mediaLink": base + "/storage/v1/b/" + rec.Bucket + "/o/" + enc + "?alt=media",
		"name":      rec.Name, "bucket": rec.Bucket, "generation": rec.Generation,
		"metageneration": rec.Metageneration, "contentType": rec.ContentType,
		"size": rec.Size, "md5Hash": rec.MD5, "etag": rec.Etag,
		"timeCreated": rec.TimeCreated, "updated": rec.Updated, "storageClass": rec.StorageClass,
	}
}

func dest(req *spi.Request) (bucket, name string) {
	bucket = str(req.Input["destinationBucket"])
	name = str(req.Input["destinationObject"])
	path := str(req.Input["_path"])
	if req.HTTP != nil && req.HTTP.URL != nil {
		path = req.HTTP.URL.Path
	}
	if db, dn := destFromPath(path); db != "" || dn != "" {
		if bucket == "" {
			bucket = db
		}
		if name == "" {
			name = dn
		}
	}
	return bucket, name
}

func readerOf(req *spi.Request) io.Reader {
	if req.Body != nil {
		return req.Body
	}
	if req.HTTP != nil && req.HTTP.Body != nil {
		return req.HTTP.Body
	}
	return bytes.NewReader(nil)
}

func blobKey(req *spi.Request, b, k string) string {
	return req.Identity.Account + "/" + req.Identity.Region + "/gcs/" + b + "/" + k
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func fault(code, msg string, status int) *spi.Fault {
	return &spi.Fault{Code: code, Message: msg, HTTPStatus: status, Fault: "client"}
}

func parseContentRange(h string, n int64) (start, end, total int64, ok bool) {
	// bytes start-end/total  or bytes */total
	if !strings.HasPrefix(h, "bytes ") {
		return 0, n - 1, n, false
	}
	h = strings.TrimPrefix(h, "bytes ")
	parts := strings.Split(h, "/")
	if len(parts) != 2 {
		return 0, n - 1, n, false
	}
	if parts[1] != "*" {
		total, _ = strconv.ParseInt(parts[1], 10, 64)
	} else {
		total = -1
	}
	if parts[0] == "*" {
		return 0, n - 1, total, true
	}
	se := strings.Split(parts[0], "-")
	if len(se) != 2 {
		return 0, n - 1, total, false
	}
	start, _ = strconv.ParseInt(se[0], 10, 64)
	end, _ = strconv.ParseInt(se[1], 10, 64)
	return start, end, total, true
}

func applyRange(b []byte, rng string) []byte {
	if !strings.HasPrefix(rng, "bytes=") {
		return b
	}
	rng = strings.TrimPrefix(rng, "bytes=")
	parts := strings.SplitN(rng, "-", 2)
	if len(parts) != 2 {
		return b
	}
	start, _ := strconv.Atoi(parts[0])
	end := len(b) - 1
	if parts[1] != "" {
		end, _ = strconv.Atoi(parts[1])
	}
	if start < 0 {
		start = 0
	}
	if end >= len(b) {
		end = len(b) - 1
	}
	if start > end {
		return b
	}
	return b[start : end+1]
}

func mustDecodeHex(h string) []byte {
	dst := make([]byte, len(h)/2)
	for i := 0; i+1 < len(h); i += 2 {
		var v byte
		for _, c := range []byte{h[i], h[i+1]} {
			v <<= 4
			switch {
			case c >= '0' && c <= '9':
				v |= c - '0'
			case c >= 'a' && c <= 'f':
				v |= c - 'a' + 10
			case c >= 'A' && c <= 'F':
				v |= c - 'A' + 10
			}
		}
		dst[i/2] = v
	}
	return dst
}
