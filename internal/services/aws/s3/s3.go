// Package s3 is the emulate-tier S3 behavior pack.
package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	bucket, key, uploadID           string
	storageClass, initiated         string
	checksumAlgorithm, checksumType string
	parts                           map[int]multipartPart
}

type multipartPart struct {
	body      []byte
	modified  string
	checksums map[string]string
}

type copySource struct {
	bucket, key, version string
	body                 io.ReadSeekCloser
	info                 spi.BlobInfo
	meta                 map[string]any
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d, mpu: map[string]*mpu{}} }

func (p *Pack) ServiceID() string { return "aws.s3" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }

func (p *Pack) Operations() []string {
	core := []string{
		"CreateBucket", "DeleteBucket", "HeadBucket", "ListBuckets", "GetBucketLocation",
		"GetBucketVersioning", "PutBucketVersioning", "GetBucketTagging", "PutBucketTagging",
		"GetBucketNotificationConfiguration", "PutBucketNotificationConfiguration",
		"GetBucketAcl", "PutBucketAcl", "GetObjectAcl", "PutObjectAcl",
		"GetBucketPolicy", "PutBucketPolicy", "DeleteBucketPolicy",
		"GetBucketCors", "PutBucketCors", "DeleteBucketCors",
		"GetBucketWebsite", "PutBucketWebsite", "DeleteBucketWebsite",
		"GetBucketLogging", "PutBucketLogging",
		"GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration", "DeleteBucketLifecycle",
		"GetBucketReplication", "PutBucketReplication",
		"GetBucketEncryption", "PutBucketEncryption", "DeleteBucketEncryption",
		"GetBucketObjectLockConfiguration", "PutBucketObjectLockConfiguration",
		"GetBucketRequestPayment", "PutBucketRequestPayment",
		"GetBucketAccelerateConfiguration", "PutBucketAccelerateConfiguration",
		"PutObject", "GetObject", "HeadObject", "DeleteObject", "DeleteObjects", "CopyObject",
		"ListObjects", "ListObjectsV2", "ListObjectVersions",
		"CreateMultipartUpload", "UploadPart", "UploadPartCopy", "CompleteMultipartUpload",
		"AbortMultipartUpload", "ListParts", "ListMultipartUploads",
		"GetObjectTagging", "PutObjectTagging",
		"PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock",
		"PutBucketOwnershipControls", "GetBucketOwnershipControls", "DeleteBucketOwnershipControls",
		"GetBucketPolicyStatus", "GetObjectAttributes",
		"DeleteBucketTagging", "DeleteObjectTagging",
		"PutObjectLegalHold", "GetObjectLegalHold", "PutObjectRetention", "GetObjectRetention",
		"RestoreObject",
		"PutBucketAnalyticsConfiguration", "GetBucketAnalyticsConfiguration", "DeleteBucketAnalyticsConfiguration", "ListBucketAnalyticsConfigurations",
		"PutBucketInventoryConfiguration", "GetBucketInventoryConfiguration", "DeleteBucketInventoryConfiguration", "ListBucketInventoryConfigurations",
		"PutBucketMetricsConfiguration", "GetBucketMetricsConfiguration", "DeleteBucketMetricsConfiguration", "ListBucketMetricsConfigurations",
		"PutBucketIntelligentTieringConfiguration", "GetBucketIntelligentTieringConfiguration", "DeleteBucketIntelligentTieringConfiguration", "ListBucketIntelligentTieringConfigurations",
		"CreateBucketMetadataConfiguration", "CreateBucketMetadataTableConfiguration", "CreateSession",
		"DeleteBucketMetadataConfiguration", "DeleteBucketMetadataTableConfiguration", "DeleteBucketReplication",
		"DeleteObjectAnnotation", "GetBucketAbac", "GetBucketMetadataConfiguration",
		"GetBucketMetadataTableConfiguration", "GetObjectAnnotation", "GetObjectLockConfiguration",
		"GetObjectTorrent", "ListDirectoryBuckets", "ListObjectAnnotations",
		"PutBucketAbac", "PutObjectAnnotation", "PutObjectLockConfiguration",
		"RenameObject", "SelectObjectContent", "UpdateBucketMetadataAnnotationTableConfiguration",
		"UpdateBucketMetadataInventoryTableConfiguration", "UpdateBucketMetadataJournalTableConfiguration",
		"UpdateObjectEncryption", "WriteGetObjectResponse",
	}
	return core
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
		return p.putObject(ctx, req, "", "")
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
	case "GetBucketAcl", "PutBucketAcl", "GetObjectAcl", "PutObjectAcl",
		"GetBucketPolicy", "PutBucketPolicy", "DeleteBucketPolicy",
		"GetBucketCors", "PutBucketCors", "DeleteBucketCors",
		"GetBucketWebsite", "PutBucketWebsite", "DeleteBucketWebsite",
		"GetBucketLogging", "PutBucketLogging",
		"GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration", "DeleteBucketLifecycle",
		"GetBucketReplication", "PutBucketReplication", "DeleteBucketReplication",
		"GetBucketEncryption", "PutBucketEncryption", "DeleteBucketEncryption",
		"GetBucketObjectLockConfiguration", "PutBucketObjectLockConfiguration",
		"GetObjectLockConfiguration", "PutObjectLockConfiguration",
		"GetBucketAbac", "PutBucketAbac",
		"GetBucketRequestPayment", "PutBucketRequestPayment",
		"GetBucketAccelerateConfiguration", "PutBucketAccelerateConfiguration",
		"PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock",
		"PutBucketOwnershipControls", "GetBucketOwnershipControls", "DeleteBucketOwnershipControls":
		return p.bucketCfg(ctx, req)
	case "GetBucketPolicyStatus":
		return p.policyStatus(ctx, req)
	case "GetObjectAttributes":
		return p.objectAttributes(ctx, req)
	case "GetBucketTagging", "GetBucketNotificationConfiguration",
		"GetObjectTagging",
		"PutBucketTagging", "PutBucketNotificationConfiguration", "PutObjectTagging",
		"DeleteBucketTagging", "DeleteObjectTagging":
		return p.emptyOK(req)
	case "PutObjectLegalHold", "GetObjectLegalHold", "PutObjectRetention", "GetObjectRetention", "RestoreObject":
		return p.objectLockExtras(ctx, req)
	case "PutBucketAnalyticsConfiguration", "GetBucketAnalyticsConfiguration", "DeleteBucketAnalyticsConfiguration", "ListBucketAnalyticsConfigurations",
		"PutBucketInventoryConfiguration", "GetBucketInventoryConfiguration", "DeleteBucketInventoryConfiguration", "ListBucketInventoryConfigurations",
		"PutBucketMetricsConfiguration", "GetBucketMetricsConfiguration", "DeleteBucketMetricsConfiguration", "ListBucketMetricsConfigurations",
		"PutBucketIntelligentTieringConfiguration", "GetBucketIntelligentTieringConfiguration", "DeleteBucketIntelligentTieringConfiguration", "ListBucketIntelligentTieringConfigurations":
		return p.namedCfg(ctx, req)
	case "ListParts":
		return p.listParts(req)
	case "ListMultipartUploads":
		return p.listMultipartUploads(ctx, req)
	case "ListObjectVersions":
		return p.listObjectVersions(ctx, req)
	case "UploadPartCopy":
		return p.uploadPartCopy(ctx, req)
	case "CreateSession":
		return p.createSession(req)
	case "RenameObject":
		return p.renameObject(ctx, req)
	case "SelectObjectContent":
		return p.selectObject(ctx, req)
	case "GetObjectTorrent":
		return p.objectTorrent(ctx, req)
	case "ListDirectoryBuckets":
		return p.listBuckets(ctx, req)
	case "WriteGetObjectResponse":
		return p.writeGetObjectResponse(ctx, req)
	case "UpdateObjectEncryption":
		return p.updateObjectEncryption(ctx, req)
	case "PutObjectAnnotation", "GetObjectAnnotation", "DeleteObjectAnnotation", "ListObjectAnnotations":
		return p.objectAnnotation(ctx, req)
	case "CreateBucketMetadataConfiguration", "GetBucketMetadataConfiguration", "DeleteBucketMetadataConfiguration",
		"CreateBucketMetadataTableConfiguration", "GetBucketMetadataTableConfiguration", "DeleteBucketMetadataTableConfiguration",
		"UpdateBucketMetadataAnnotationTableConfiguration", "UpdateBucketMetadataInventoryTableConfiguration",
		"UpdateBucketMetadataJournalTableConfiguration":
		return p.metadataCfg(ctx, req)
	default:
		return nil, spi.NotImplemented("aws.s3", req.Operation, "emulate")
	}
}

func (p *Pack) route(req *spi.Request) string {
	r := req.HTTP
	path := strings.TrimPrefix(r.URL.Path, "/")
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
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	req.Input["Bucket"] = bucket
	if key != "" {
		req.Input["Key"] = key
	}
	q := r.URL.Query()
	if a := q.Get("Action"); a != "" {
		return a
	}
	has := func(k string) bool { _, ok := q[k]; return ok }
	if v := q.Get("versionId"); v != "" {
		req.Input["VersionId"] = v
	}
	if v := q.Get("max-keys"); v != "" {
		req.Input["MaxKeys"] = v
	}
	if v := q.Get("continuation-token"); v != "" {
		req.Input["ContinuationToken"] = v
	}
	if v := q.Get("start-after"); v != "" {
		req.Input["StartAfter"] = v
	}
	if v := q.Get("encoding-type"); v != "" {
		req.Input["EncodingType"] = v
	}
	m := r.Method
	switch {
	case has("tagging"):
		if key == "" {
			if m == http.MethodPut {
				return "PutBucketTagging"
			}
			if m == http.MethodDelete {
				return "DeleteBucketTagging"
			}
			return "GetBucketTagging"
		}
		if m == http.MethodPut {
			return "PutObjectTagging"
		}
		if m == http.MethodDelete {
			return "DeleteObjectTagging"
		}
		return "GetObjectTagging"
	case has("notification"):
		if m == http.MethodPut {
			return "PutBucketNotificationConfiguration"
		}
		return "GetBucketNotificationConfiguration"
	case has("versioning"):
		if m == http.MethodPut {
			return "PutBucketVersioning"
		}
		return "GetBucketVersioning"
	case has("acl"):
		if key == "" {
			return putGetDel(m, "PutBucketAcl", "GetBucketAcl", "")
		}
		return putGetDel(m, "PutObjectAcl", "GetObjectAcl", "")
	case has("policy"):
		return putGetDel(m, "PutBucketPolicy", "GetBucketPolicy", "DeleteBucketPolicy")
	case has("cors"):
		return putGetDel(m, "PutBucketCors", "GetBucketCors", "DeleteBucketCors")
	case has("website"):
		return putGetDel(m, "PutBucketWebsite", "GetBucketWebsite", "DeleteBucketWebsite")
	case has("logging"):
		return putGetDel(m, "PutBucketLogging", "GetBucketLogging", "")
	case has("lifecycle"):
		return putGetDel(m, "PutBucketLifecycleConfiguration", "GetBucketLifecycleConfiguration", "DeleteBucketLifecycle")
	case has("replication"):
		return putGetDel(m, "PutBucketReplication", "GetBucketReplication", "DeleteBucketReplication")
	case has("session"):
		return "CreateSession"
	case has("select"):
		return "SelectObjectContent"
	case has("torrent"):
		return "GetObjectTorrent"
	case has("abac"):
		return putGetDel(m, "PutBucketAbac", "GetBucketAbac", "")
	case has("metadataTable"):
		return putGetDel(m, "CreateBucketMetadataTableConfiguration", "GetBucketMetadataTableConfiguration", "DeleteBucketMetadataTableConfiguration")
	case has("metadataConfiguration") || has("metadata"):
		if m == http.MethodPost {
			if has("inventory") {
				return "UpdateBucketMetadataInventoryTableConfiguration"
			}
			if has("journal") {
				return "UpdateBucketMetadataJournalTableConfiguration"
			}
			return "UpdateBucketMetadataAnnotationTableConfiguration"
		}
		return putGetDel(m, "CreateBucketMetadataConfiguration", "GetBucketMetadataConfiguration", "DeleteBucketMetadataConfiguration")
	case has("annotation"):
		if key == "" {
			return "ListObjectAnnotations"
		}
		return putGetDel(m, "PutObjectAnnotation", "GetObjectAnnotation", "DeleteObjectAnnotation")
	case has("rename") || r.Header.Get("x-amz-rename-source") != "":
		return "RenameObject"
	case has("x-amz-request-route") || r.Header.Get("x-amz-request-route") != "" || has("WriteGetObjectResponse"):
		return "WriteGetObjectResponse"
	case has("encryption"):
		return putGetDel(m, "PutBucketEncryption", "GetBucketEncryption", "DeleteBucketEncryption")
	case has("object-lock"):
		return putGetDel(m, "PutBucketObjectLockConfiguration", "GetBucketObjectLockConfiguration", "")
	case has("requestPayment"):
		return putGetDel(m, "PutBucketRequestPayment", "GetBucketRequestPayment", "")
	case has("accelerate"):
		return putGetDel(m, "PutBucketAccelerateConfiguration", "GetBucketAccelerateConfiguration", "")
	case has("publicAccessBlock"):
		return putGetDel(m, "PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock")
	case has("ownershipControls"):
		return putGetDel(m, "PutBucketOwnershipControls", "GetBucketOwnershipControls", "DeleteBucketOwnershipControls")
	case has("policyStatus") && m == http.MethodGet:
		return "GetBucketPolicyStatus"
	case has("attributes") && m == http.MethodGet:
		return "GetObjectAttributes"
	case has("legal-hold"):
		return putGetDel(m, "PutObjectLegalHold", "GetObjectLegalHold", "")
	case has("retention"):
		return putGetDel(m, "PutObjectRetention", "GetObjectRetention", "")
	case has("restore") && m == http.MethodPost:
		return "RestoreObject"
	case has("analytics"):
		if id := q.Get("id"); id != "" {
			req.Input["Id"] = id
			return putGetDel(m, "PutBucketAnalyticsConfiguration", "GetBucketAnalyticsConfiguration", "DeleteBucketAnalyticsConfiguration")
		}
		return "ListBucketAnalyticsConfigurations"
	case has("inventory"):
		if id := q.Get("id"); id != "" {
			req.Input["Id"] = id
			return putGetDel(m, "PutBucketInventoryConfiguration", "GetBucketInventoryConfiguration", "DeleteBucketInventoryConfiguration")
		}
		return "ListBucketInventoryConfigurations"
	case has("metrics"):
		if id := q.Get("id"); id != "" {
			req.Input["Id"] = id
			return putGetDel(m, "PutBucketMetricsConfiguration", "GetBucketMetricsConfiguration", "DeleteBucketMetricsConfiguration")
		}
		return "ListBucketMetricsConfigurations"
	case has("intelligent-tiering"):
		if id := q.Get("id"); id != "" {
			req.Input["Id"] = id
			return putGetDel(m, "PutBucketIntelligentTieringConfiguration", "GetBucketIntelligentTieringConfiguration", "DeleteBucketIntelligentTieringConfiguration")
		}
		return "ListBucketIntelligentTieringConfigurations"
	case has("location") && m == http.MethodGet:
		return "GetBucketLocation"
	case has("versions") && m == http.MethodGet:
		return "ListObjectVersions"
	case has("delete") && m == http.MethodPost:
		return "DeleteObjects"
	case has("uploads") && m == http.MethodPost:
		return "CreateMultipartUpload"
	case has("uploads") && m == http.MethodGet:
		return "ListMultipartUploads"
	case has("partNumber") && m == http.MethodPut:
		if r.Header.Get("x-amz-copy-source") != "" {
			return "UploadPartCopy"
		}
		return "UploadPart"
	case has("uploadId") && m == http.MethodGet:
		return "ListParts"
	case has("uploadId") && m == http.MethodPost:
		return "CompleteMultipartUpload"
	case has("uploadId") && m == http.MethodDelete:
		return "AbortMultipartUpload"
	case m == http.MethodPut && r.Header.Get("x-amz-copy-source") != "":
		return "CopyObject"
	case m == http.MethodGet && bucket == "":
		return "ListBuckets"
	case m == http.MethodHead && key == "":
		return "HeadBucket"
	case m == http.MethodPut && key == "":
		return "CreateBucket"
	case m == http.MethodDelete && key == "":
		return "DeleteBucket"
	case m == http.MethodPut && key != "":
		return "PutObject"
	case m == http.MethodGet && key != "":
		return "GetObject"
	case m == http.MethodHead && key != "":
		return "HeadObject"
	case m == http.MethodDelete && key != "":
		return "DeleteObject"
	case m == http.MethodGet && key == "":
		if q.Get("list-type") == "1" {
			return "ListObjects"
		}
		return "ListObjectsV2"
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
	location, _ := json.Marshal(map[string]any{"account": req.Identity.Account, "region": req.Identity.Region})
	_ = p.deps.Store.Scope("_mirror", "global").Collection("s3buckets").Put(ctx, b, location)
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
	_ = p.deps.Store.Scope("_mirror", "global").Collection("s3buckets").Delete(ctx, b)
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

func (p *Pack) putObject(ctx context.Context, req *spi.Request, etag, checksumType string) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	if err := p.checkWritePreconditions(ctx, req, b, key); err != nil {
		return nil, err
	}
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	if checksumType == "" {
		checksumType = "FULL_OBJECT"
	}
	if checksumType == "FULL_OBJECT" {
		if err := validateChecksum(req, body); err != nil {
			return nil, err
		}
	}
	provided := providedChecksums(req)
	info, err := p.deps.Blobs.Put(ctx, blobKey(req, b, key), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if etag == "" {
		etag = `"` + info.MD5 + `"`
	}
	mtime := p.deps.Clock.Now().UTC().Format(http.TimeFormat)
	storageClass := str(req.Input["StorageClass"])
	if storageClass == "" {
		storageClass = "STANDARD"
	}
	vid := ""
	if p.versioningEnabled(ctx, req, b) {
		vid = p.deps.Rand.Hex(8)
		_, _ = p.deps.Blobs.Put(ctx, blobKey(req, b, key)+"@"+vid, bytes.NewReader(body))
		versionMeta := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "versionId": vid, "mtime": mtime, "key": key, "storageClass": storageClass}
		if len(provided) > 0 {
			versionMeta["checksums"] = provided
			versionMeta["checksumType"] = checksumType
		}
		vm, _ := json.Marshal(versionMeta)
		_ = p.col(req, "versions").Put(ctx, b+"/"+key+"/"+vid, vm)
	}
	metaDoc := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "mtime": mtime, "versionId": vid, "deleteMarker": false, "storageClass": storageClass}
	if len(provided) > 0 {
		metaDoc["checksums"] = provided
		metaDoc["checksumType"] = checksumType
	}
	meta, _ := json.Marshal(metaDoc)
	_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
	if tags := requestTags(req); len(tags) > 0 {
		raw, _ := json.Marshal(tagSet(tags))
		_ = p.col(req, "tags").Put(ctx, b+"/"+key, raw)
	}
	h := http.Header{}
	h.Set("ETag", etag)
	if vid != "" {
		h.Set("x-amz-version-id", vid)
	}
	for header, value := range provided {
		h.Set(header, value)
	}
	if len(provided) > 0 {
		h.Set("x-amz-checksum-type", checksumType)
	}
	if status := p.replicateObject(ctx, req, b, key, body, metaDoc); status != "" {
		metaDoc["replicationStatus"] = status
		meta, _ = json.Marshal(metaDoc)
		_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
		h.Set("x-amz-replication-status", status)
	}
	p.notify(ctx, req, b, key, "ObjectCreated:Put")
	return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"ETag": etag}}, nil
}

func (p *Pack) getObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	wantVer := str(req.Input["VersionId"])
	meta, exists := p.objectMetadata(ctx, req, b, key, wantVer)
	if !exists {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return nil, deleteMarkerReadFault(meta, wantVer != "")
	}
	bk := blobKey(req, b, key)
	if wantVer != "" {
		bk = bk + "@" + wantVer
	}
	rc, info, err := p.deps.Blobs.Get(ctx, bk)
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	h := http.Header{}
	etag := objectETag(meta, info.MD5)
	h.Set("ETag", etag)
	h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	mtime := str(meta["mtime"])
	if mtime == "" {
		mtime = p.deps.Clock.Now().UTC().Format(http.TimeFormat)
	}
	h.Set("Last-Modified", mtime)
	if vid := str(meta["versionId"]); vid != "" {
		h.Set("x-amz-version-id", vid)
	}
	if encryption := str(meta["serverSideEncryption"]); encryption != "" {
		h.Set("x-amz-server-side-encryption", encryption)
	}
	if keyID := str(meta["ssekmsKeyId"]); keyID != "" {
		h.Set("x-amz-server-side-encryption-aws-kms-key-id", keyID)
	}
	if requestCondition(req, "ChecksumMode", "x-amz-checksum-mode") == "ENABLED" {
		setChecksumHeaders(h, meta)
	}
	setReplicationHeaders(h, meta)
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if req.HTTP != nil {
		if im := req.HTTP.Header.Get("If-Match"); im != "" && im != etag && im != strings.Trim(etag, `"`) {
			return nil, &spi.Fault{Code: "PreconditionFailed", HTTPStatus: 412, Fault: "client"}
		}
		if inm := req.HTTP.Header.Get("If-None-Match"); inm != "" && (inm == etag || inm == "*" || inm == strings.Trim(etag, `"`)) {
			return &spi.Response{Status: 304, Headers: h}, nil
		}
		if ims := req.HTTP.Header.Get("If-Modified-Since"); ims != "" {
			if since, err := http.ParseTime(ims); err == nil {
				if mt, err := time.Parse(http.TimeFormat, mtime); err == nil && !mt.After(since) {
					return &spi.Response{Status: 304, Headers: h}, nil
				}
			}
		}
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
	wantVer := str(req.Input["VersionId"])
	meta, exists := p.objectMetadata(ctx, req, b, key, wantVer)
	if !exists {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return nil, deleteMarkerReadFault(meta, wantVer != "")
	}
	bk := blobKey(req, b, key)
	if wantVer != "" {
		bk += "@" + wantVer
	}
	info, err := p.deps.Blobs.Stat(ctx, bk)
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	h := http.Header{}
	h.Set("ETag", objectETag(meta, info.MD5))
	h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	h.Set("Last-Modified", str(meta["mtime"]))
	if version := str(meta["versionId"]); version != "" {
		h.Set("x-amz-version-id", version)
	}
	if requestCondition(req, "ChecksumMode", "x-amz-checksum-mode") == "ENABLED" {
		setChecksumHeaders(h, meta)
	}
	setReplicationHeaders(h, meta)
	return &spi.Response{Status: 200, Headers: h}, nil
}

func (p *Pack) deleteObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	wantVer := str(req.Input["VersionId"])
	if p.versioningEnabled(ctx, req, b) && wantVer == "" {
		vid := p.deps.Rand.Hex(8)
		mtime := p.deps.Clock.Now().UTC().Format(http.TimeFormat)
		meta, _ := json.Marshal(map[string]any{"deleteMarker": true, "versionId": vid, "mtime": mtime, "key": key})
		_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
		_ = p.col(req, "versions").Put(ctx, b+"/"+key+"/"+vid, meta)
		h := http.Header{}
		h.Set("x-amz-delete-marker", "true")
		h.Set("x-amz-version-id", vid)
		if status := p.replicateDeleteMarker(ctx, req, b, key, meta); status != "" {
			h.Set("x-amz-replication-status", status)
		}
		return &spi.Response{Status: 204, Headers: h}, nil
	}
	if wantVer != "" {
		_ = p.deps.Blobs.Delete(ctx, blobKey(req, b, key)+"@"+wantVer)
		_ = p.col(req, "versions").Delete(ctx, b+"/"+key+"/"+wantVer)
		return &spi.Response{Status: 204}, nil
	}
	_ = p.deps.Blobs.Delete(ctx, blobKey(req, b, key))
	_ = p.col(req, "objects").Delete(ctx, b+"/"+key)
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) deleteObjects(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	objs, _ := req.Input["Objects"].([]any)
	if objs == nil {
		if d, ok := req.Input["Delete"].(map[string]any); ok {
			objs, _ = d["Objects"].([]any)
		}
	}
	var deleted []any
	for _, o := range objs {
		m, _ := o.(map[string]any)
		key := str(m["Key"])
		if key == "" {
			continue
		}
		child := *req
		child.Input = cloneMap(req.Input)
		child.Input["Key"] = key
		if versionID := str(m["VersionId"]); versionID != "" {
			child.Input["VersionId"] = versionID
		} else {
			delete(child.Input, "VersionId")
		}
		resp, err := p.deleteObject(ctx, &child)
		if err != nil {
			return nil, err
		}
		item := map[string]any{"Key": key}
		if resp.Headers != nil {
			if versionID := resp.Headers.Get("x-amz-version-id"); versionID != "" {
				item["DeleteMarkerVersionId"] = versionID
				item["DeleteMarker"] = true
			}
		}
		deleted = append(deleted, item)
	}
	return &spi.Response{Output: map[string]any{"Deleted": deleted}}, nil
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
		contents = append(contents, map[string]any{"Key": key, "Size": meta["size"], "ETag": meta["etag"], "LastModified": meta["mtime"], "StorageClass": meta["storageClass"]})
	}
	var prefixes []any
	for pfx := range common {
		prefixes = append(prefixes, map[string]any{"Prefix": pfx})
	}
	sort.Slice(contents, func(i, j int) bool {
		return str(asMap(contents[i])["Key"]) < str(asMap(contents[j])["Key"])
	})
	maxKeys := 1000
	if n := asInt(req.Input["MaxKeys"]); n > 0 {
		maxKeys = n
	}
	token := str(req.Input["ContinuationToken"])
	if token == "" {
		token = str(req.Input["StartAfter"])
	}
	if token != "" {
		var rest []any
		for _, c := range contents {
			if str(asMap(c)["Key"]) > token {
				rest = append(rest, c)
			}
		}
		contents = rest
	}
	truncated := false
	next := ""
	if len(contents) > maxKeys {
		truncated = true
		next = str(asMap(contents[maxKeys-1])["Key"])
		contents = contents[:maxKeys]
	}
	out := map[string]any{
		"Name": b, "Prefix": prefix, "Delimiter": delim,
		"IsTruncated": truncated, "MaxKeys": maxKeys,
		"Contents": contents, "CommonPrefixes": prefixes, "KeyCount": len(contents),
	}
	if next != "" {
		out["NextContinuationToken"] = next
	}
	if str(req.Input["EncodingType"]) == "url" || str(req.Input["encoding-type"]) == "url" {
		for _, c := range contents {
			m := asMap(c)
			m["Key"] = url.QueryEscape(str(m["Key"]))
		}
		out["EncodingType"] = "url"
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) copyObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	source, err := p.openCopySource(ctx, req)
	if err != nil {
		return nil, err
	}
	defer source.body.Close()
	if err := checkCopySourcePreconditions(req, objectETag(source.meta, source.info.MD5), str(source.meta["mtime"])); err != nil {
		return nil, err
	}
	directive := str(req.Input["TaggingDirective"])
	if directive == "" && req.HTTP != nil {
		directive = req.HTTP.Header.Get("x-amz-tagging-directive")
	}
	if !strings.EqualFold(directive, "REPLACE") {
		values := url.Values{}
		for key, value := range p.storedTags(ctx, req, source.bucket, source.key) {
			values.Set(key, value)
		}
		req.Input["Tagging"] = values.Encode()
	}
	req.Body = source.body
	response, err := p.putObject(ctx, req, "", "")
	if err == nil && source.version != "" {
		response.Headers.Set("x-amz-copy-source-version-id", source.version)
	}
	return response, err
}

func (p *Pack) createMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	algorithm := strings.ToUpper(requestCondition(req, "ChecksumAlgorithm", "x-amz-checksum-algorithm"))
	checksumType := strings.ToUpper(requestCondition(req, "ChecksumType", "x-amz-checksum-type"))
	if algorithm == "" {
		algorithm, checksumType = "CRC64NVME", "FULL_OBJECT"
	} else if checksumType == "" {
		checksumType = "COMPOSITE"
	}
	if err := validateMultipartChecksumContract(req, algorithm, checksumType); err != nil {
		return nil, err
	}
	storageClass := str(req.Input["StorageClass"])
	if storageClass == "" {
		storageClass = "STANDARD"
	}
	id := p.deps.Rand.Hex(16)
	p.mu.Lock()
	p.mpu[id] = &mpu{bucket: b, key: key, uploadID: id, storageClass: storageClass, initiated: p.deps.Clock.Now().UTC().Format(time.RFC3339Nano), checksumAlgorithm: algorithm, checksumType: checksumType, parts: map[int]multipartPart{}}
	p.mu.Unlock()
	h := http.Header{}
	h.Set("x-amz-checksum-algorithm", algorithm)
	h.Set("x-amz-checksum-type", checksumType)
	return &spi.Response{Headers: h, Output: map[string]any{"Bucket": b, "Key": key, "UploadId": id, "ChecksumAlgorithm": algorithm, "ChecksumType": checksumType}}, nil
}

func (p *Pack) uploadPartCopy(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	source, err := p.openCopySource(ctx, req)
	if err != nil {
		return nil, err
	}
	defer source.body.Close()
	if err := checkCopySourcePreconditions(req, objectETag(source.meta, source.info.MD5), str(source.meta["mtime"])); err != nil {
		return nil, err
	}
	req.Body = source.body
	if rawRange := requestCondition(req, "CopySourceRange", "x-amz-copy-source-range"); rawRange != "" {
		body, _ := io.ReadAll(source.body)
		body, err = applyCopySourceRange(body, rawRange)
		if err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	response, err := p.uploadPart(ctx, req)
	if err == nil && source.version != "" {
		response.Headers.Set("x-amz-copy-source-version-id", source.version)
	}
	return response, err
}

func (p *Pack) uploadPart(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	pn := partNumber(req)
	if pn < 1 || pn > 10000 {
		return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	p.mu.Lock()
	u := p.mpu[id]
	if !matchesMultipartUpload(u, req) {
		p.mu.Unlock()
		return nil, &spi.Fault{Code: "NoSuchUpload", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	algorithm := u.checksumAlgorithm
	p.mu.Unlock()
	if requested := strings.ToUpper(requestCondition(req, "ChecksumAlgorithm", "x-amz-sdk-checksum-algorithm")); requested != "" && requested != algorithm {
		return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	checksum, _ := checksumByAlgorithm(algorithm)
	if err := validateMultipartPartChecksum(req, checksum, body); err != nil {
		return nil, err
	}
	value := checksumValue(checksum.input, body)
	provided := map[string]string{checksum.header: value}
	p.mu.Lock()
	u = p.mpu[id]
	if !matchesMultipartUpload(u, req) {
		p.mu.Unlock()
		return nil, &spi.Fault{Code: "NoSuchUpload", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	u.parts[pn] = multipartPart{body: body, modified: p.deps.Clock.Now().UTC().Format(time.RFC3339), checksums: provided}
	p.mu.Unlock()
	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	h := http.Header{}
	h.Set("ETag", etag)
	for header, value := range provided {
		h.Set(header, value)
	}
	return &spi.Response{Headers: h, Output: map[string]any{"ETag": etag}}, nil
}

func (p *Pack) completeMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	p.mu.Lock()
	u := p.mpu[id]
	var bucket, key string
	stored := map[int]multipartPart{}
	if u != nil {
		bucket, key = u.bucket, u.key
		for number, part := range u.parts {
			stored[number] = part
		}
	}
	p.mu.Unlock()
	if !matchesMultipartUpload(u, req) {
		return nil, &spi.Fault{Code: "NoSuchUpload", HTTPStatus: 404, Fault: "client"}
	}
	parts := asSlice(asMap(req.Input["MultipartUpload"])["Parts"])
	if len(parts) == 0 {
		return nil, &spi.Fault{Code: "InvalidPart", HTTPStatus: 400, Fault: "client"}
	}
	var buf bytes.Buffer
	var md5s []byte
	var partChecksums []byte
	previous := 0
	checksum, _ := checksumByAlgorithm(u.checksumAlgorithm)
	if requestedType := strings.ToUpper(requestCondition(req, "ChecksumType", "x-amz-checksum-type")); requestedType != "" && requestedType != u.checksumType {
		return nil, &spi.Fault{Code: "BadDigest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	for index, completed := range parts {
		item := asMap(completed)
		number := asInt(item["PartNumber"])
		if number < 1 || number > 10000 {
			return nil, &spi.Fault{Code: "InvalidPart", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		if number <= previous {
			return nil, &spi.Fault{Code: "InvalidPartOrder", HTTPStatus: 400, Fault: "client"}
		}
		part, exists := stored[number]
		s := md5.Sum(part.body)
		if !exists || strings.Trim(strings.TrimSpace(str(item["ETag"])), `"`) != hex.EncodeToString(s[:]) {
			return nil, &spi.Fault{Code: "InvalidPart", HTTPStatus: 400, Fault: "client"}
		}
		if index < len(parts)-1 && len(part.body) < 5<<20 {
			return nil, &spi.Fault{Code: "EntityTooSmall", HTTPStatus: 400, Fault: "client"}
		}
		if u.checksumType == "COMPOSITE" && number != index+1 {
			return nil, &spi.Fault{Code: "InternalError", HTTPStatus: http.StatusInternalServerError, Fault: "server"}
		}
		partChecksum := part.checksums[checksum.header]
		if supplied := str(item[checksum.input]); supplied != "" && supplied != partChecksum {
			return nil, &spi.Fault{Code: "InvalidPart", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		decoded, _ := base64.StdEncoding.DecodeString(partChecksum)
		partChecksums = append(partChecksums, decoded...)
		buf.Write(part.body)
		md5s = append(md5s, s[:]...)
		previous = number
	}
	if value := requestCondition(req, "MpuObjectSize", "x-amz-mp-object-size"); value != "" {
		size, err := strconv.ParseInt(value, 10, 64)
		if err != nil || size != int64(buf.Len()) {
			return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: 400, Fault: "client"}
		}
	}
	sum := md5.Sum(md5s)
	etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(sum[:]), len(parts))
	objectChecksum := checksumValue(checksum.input, buf.Bytes())
	if u.checksumType == "COMPOSITE" {
		objectChecksum = checksumValue(checksum.input, partChecksums) + fmt.Sprintf("-%d", len(parts))
	}
	if supplied := requestCondition(req, checksum.input, checksum.header); supplied != "" && supplied != objectChecksum {
		return nil, &spi.Fault{Code: "BadDigest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	for _, other := range checksums {
		if other.algorithm != checksum.algorithm && requestCondition(req, other.input, other.header) != "" {
			return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
	}
	req.Input[checksum.input], req.Input["ChecksumType"] = objectChecksum, u.checksumType
	req.Input["Bucket"], req.Input["Key"] = bucket, key
	req.Body = io.NopCloser(&buf)
	resp, err := p.putObject(ctx, req, etag, u.checksumType)
	if err != nil {
		return nil, err
	}
	if resp.Headers == nil {
		resp.Headers = http.Header{}
	}
	resp.Headers.Set("ETag", etag)
	resp.Output = map[string]any{"Bucket": bucket, "Key": key, "ETag": etag}
	resp.Output[checksum.input] = objectChecksum
	resp.Output["ChecksumType"] = u.checksumType
	p.mu.Lock()
	delete(p.mpu, id)
	p.mu.Unlock()
	return resp, nil
}

func (p *Pack) listParts(req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	marker := asInt(req.Input["PartNumberMarker"])
	maxParts := 1000
	if _, provided := req.Input["MaxParts"]; provided {
		maxParts = asInt(req.Input["MaxParts"])
	}
	if req.HTTP != nil {
		if raw := req.HTTP.URL.Query().Get("part-number-marker"); raw != "" {
			var err error
			marker, err = strconv.Atoi(raw)
			if err != nil {
				return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
		}
		if raw := req.HTTP.URL.Query().Get("max-parts"); raw != "" {
			var err error
			maxParts, err = strconv.Atoi(raw)
			if err != nil {
				return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
		}
	}
	if marker < 0 || maxParts < 0 || maxParts > 1000 {
		return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	u := p.mpu[id]
	if !matchesMultipartUpload(u, req) {
		return nil, &spi.Fault{Code: "NoSuchUpload", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	numbers := make([]int, 0, len(u.parts))
	for number := range u.parts {
		if number > marker {
			numbers = append(numbers, number)
		}
	}
	sort.Ints(numbers)
	truncated := len(numbers) > maxParts
	if truncated {
		numbers = numbers[:maxParts]
	}
	parts := make([]any, 0, len(numbers))
	for _, number := range numbers {
		part := u.parts[number]
		sum := md5.Sum(part.body)
		row := map[string]any{"PartNumber": number, "ETag": `"` + hex.EncodeToString(sum[:]) + `"`, "Size": len(part.body), "LastModified": part.modified}
		for _, checksum := range checksums {
			if value := part.checksums[checksum.header]; value != "" {
				row[checksum.input] = value
			}
		}
		parts = append(parts, row)
	}
	out := map[string]any{
		"Bucket": b, "Key": key, "UploadId": id, "PartNumberMarker": marker,
		"MaxParts": maxParts, "IsTruncated": truncated, "Parts": parts, "StorageClass": u.storageClass,
		"ChecksumAlgorithm": u.checksumAlgorithm, "ChecksumType": u.checksumType,
	}
	if truncated {
		out["NextPartNumberMarker"] = marker
		if len(numbers) > 0 {
			out["NextPartNumberMarker"] = numbers[len(numbers)-1]
		}
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) listMultipartUploads(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, bucket); err != nil {
		return nil, err
	}
	parameter := func(input, query string) string {
		if value := str(req.Input[input]); value != "" {
			return value
		}
		if req.HTTP != nil {
			return req.HTTP.URL.Query().Get(query)
		}
		return ""
	}
	prefix := parameter("Prefix", "prefix")
	delimiter := parameter("Delimiter", "delimiter")
	keyMarker := parameter("KeyMarker", "key-marker")
	uploadMarker := parameter("UploadIdMarker", "upload-id-marker")
	encoding := parameter("EncodingType", "encoding-type")
	maxUploads := 1000
	if _, provided := req.Input["MaxUploads"]; provided {
		maxUploads = asInt(req.Input["MaxUploads"])
	}
	if raw := parameter("", "max-uploads"); raw != "" {
		var err error
		maxUploads, err = strconv.Atoi(raw)
		if err != nil {
			return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
	}
	if maxUploads < 1 || maxUploads > 1000 {
		return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}

	type uploadListing struct{ key, id, initiated, storageClass string }
	p.mu.Lock()
	uploads := make([]uploadListing, 0, len(p.mpu))
	for id, upload := range p.mpu {
		if upload.bucket == bucket && strings.HasPrefix(upload.key, prefix) {
			uploads = append(uploads, uploadListing{upload.key, id, upload.initiated, upload.storageClass})
		}
	}
	p.mu.Unlock()
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].key != uploads[j].key {
			return uploads[i].key < uploads[j].key
		}
		if uploads[i].initiated != uploads[j].initiated {
			return uploads[i].initiated < uploads[j].initiated
		}
		return uploads[i].id < uploads[j].id
	})

	type entry struct {
		key, uploadID string
		row           map[string]any
		common        bool
	}
	entries := make([]entry, 0, len(uploads))
	common := map[string]bool{}
	markerPassed := keyMarker == ""
	for _, upload := range uploads {
		if !markerPassed {
			switch {
			case upload.key > keyMarker:
				markerPassed = true
			case upload.key == keyMarker && uploadMarker != "" && upload.id == uploadMarker:
				markerPassed = true
				continue
			default:
				continue
			}
		}
		if delimiter != "" {
			remainder := strings.TrimPrefix(upload.key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				value := prefix + remainder[:index+len(delimiter)]
				if value <= keyMarker || common[value] {
					continue
				}
				common[value] = true
				entries = append(entries, entry{key: value, row: map[string]any{"Prefix": value}, common: true})
				continue
			}
		}
		identity := map[string]any{"ID": req.Identity.Account}
		entries = append(entries, entry{key: upload.key, uploadID: upload.id, row: map[string]any{
			"Key": upload.key, "UploadId": upload.id, "Initiated": upload.initiated,
			"StorageClass": upload.storageClass, "Initiator": identity, "Owner": identity,
		}})
	}
	truncated := len(entries) > maxUploads
	if truncated {
		entries = entries[:maxUploads]
	}
	listed, prefixes := []any{}, []any{}
	for _, item := range entries {
		if item.common {
			prefixes = append(prefixes, item.row)
		} else {
			listed = append(listed, item.row)
		}
	}
	out := map[string]any{
		"Bucket": bucket, "Prefix": prefix, "KeyMarker": keyMarker, "UploadIdMarker": uploadMarker,
		"MaxUploads": maxUploads, "IsTruncated": truncated, "Uploads": listed, "CommonPrefixes": prefixes,
	}
	if delimiter != "" {
		out["Delimiter"] = delimiter
	}
	if truncated {
		last := entries[len(entries)-1]
		out["NextKeyMarker"] = last.key
		if last.uploadID != "" {
			out["NextUploadIdMarker"] = last.uploadID
		}
	}
	if encoding == "url" {
		encode := func(value string) string { return strings.ReplaceAll(url.QueryEscape(value), "+", "%20") }
		for _, item := range append(listed, prefixes...) {
			row := asMap(item)
			for _, field := range []string{"Key", "Prefix"} {
				if value := str(row[field]); value != "" {
					row[field] = encode(value)
				}
			}
		}
		for _, field := range []string{"Prefix", "Delimiter", "KeyMarker", "NextKeyMarker"} {
			if value := str(out[field]); value != "" {
				out[field] = encode(value)
			}
		}
		out["EncodingType"] = "url"
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) listObjectVersions(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	kvs, _, _ := p.col(req, "versions").List(ctx, b+"/", "", 0)
	var versions, markers []any
	for _, kv := range kvs {
		var meta map[string]any
		_ = json.Unmarshal(kv.Value, &meta)
		key := str(meta["key"])
		if key == "" {
			parts := strings.Split(strings.TrimPrefix(kv.Key, b+"/"), "/")
			if len(parts) > 0 {
				key = strings.Join(parts[:len(parts)-1], "/")
			}
		}
		row := map[string]any{"Key": key, "VersionId": meta["versionId"], "ETag": meta["etag"], "Size": meta["size"]}
		if truthy(meta["deleteMarker"]) {
			markers = append(markers, row)
			continue
		}
		versions = append(versions, row)
	}
	if len(versions) == 0 && len(markers) == 0 {
		resp, err := p.listObjects(ctx, req)
		if err != nil {
			return nil, err
		}
		resp.Output["Versions"] = resp.Output["Contents"]
		return resp, nil
	}
	return &spi.Response{Output: map[string]any{"Name": b, "Versions": versions, "DeleteMarkers": markers}}, nil
}

func (p *Pack) abortMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := mpuID(req)
	p.mu.Lock()
	if !matchesMultipartUpload(p.mpu[id], req) {
		p.mu.Unlock()
		return nil, &spi.Fault{Code: "NoSuchUpload", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	delete(p.mpu, id)
	p.mu.Unlock()
	return &spi.Response{Status: 204}, nil
}

func matchesMultipartUpload(upload *mpu, req *spi.Request) bool {
	bucket, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	return upload != nil && (bucket == "" || upload.bucket == bucket) && (key == "" || upload.key == key)
}

func (p *Pack) versioning(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if req.Operation == "PutBucketVersioning" {
		st := str(req.Input["Status"])
		if st == "" {
			st = "Enabled"
		}
		_ = p.col(req, "versioning").Put(ctx, b, []byte(st))
		return &spi.Response{Status: 200, Output: map[string]any{"Status": st}}, nil
	}
	raw, ok, _ := p.col(req, "versioning").Get(ctx, b)
	st := "Suspended"
	if ok && len(raw) > 0 {
		st = string(raw)
	}
	return &spi.Response{Output: map[string]any{"Status": st}}, nil
}

func putGetDel(method, put, get, del string) string {
	switch method {
	case http.MethodPut:
		return put
	case http.MethodDelete:
		if del != "" {
			return del
		}
		return get
	default:
		return get
	}
}

func (p *Pack) bucketCfg(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	kind, miss := cfgKind(req.Operation)
	key := b + "/" + kind
	if keyObj := str(req.Input["Key"]); keyObj != "" {
		key = b + "/" + keyObj + "/" + kind
	}
	col := p.col(req, "bktcfg")
	if strings.HasPrefix(req.Operation, "Put") {
		doc := map[string]any{}
		for k, v := range req.Input {
			if k == "Bucket" || k == "Key" {
				continue
			}
			doc[k] = v
		}
		raw, _ := json.Marshal(doc)
		_ = col.Put(ctx, key, raw)
		return &spi.Response{Status: 200, Output: doc}, nil
	}
	if strings.HasPrefix(req.Operation, "Delete") {
		_ = col.Delete(ctx, key)
		return &spi.Response{Status: 204}, nil
	}
	raw, ok, _ := col.Get(ctx, key)
	if !ok {
		if req.Operation == "GetBucketAcl" || req.Operation == "GetObjectAcl" {
			return &spi.Response{Output: map[string]any{
				"Owner":  map[string]any{"ID": req.Identity.Account, "DisplayName": "mirror"},
				"Grants": []any{map[string]any{"Grantee": map[string]any{"ID": req.Identity.Account, "Type": "CanonicalUser"}, "Permission": "FULL_CONTROL"}},
			}}, nil
		}
		if req.Operation == "GetBucketLogging" {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		if req.Operation == "GetBucketRequestPayment" {
			return &spi.Response{Output: map[string]any{"Payer": "BucketOwner"}}, nil
		}
		if req.Operation == "GetBucketAccelerateConfiguration" {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		if miss != nil {
			return nil, miss
		}
		return &spi.Response{Output: map[string]any{}}, nil
	}
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	return &spi.Response{Status: 200, Output: doc}, nil
}

func cfgKind(op string) (string, *spi.Fault) {
	n := func(code, msg string) *spi.Fault {
		return &spi.Fault{Code: code, Message: msg, HTTPStatus: 404, Fault: "client"}
	}
	switch {
	case strings.Contains(op, "Policy"):
		return "policy", n("NoSuchBucketPolicy", "The bucket policy does not exist")
	case strings.Contains(op, "Cors"):
		return "cors", n("NoSuchCORSConfiguration", "The CORS configuration does not exist")
	case strings.Contains(op, "Website"):
		return "website", n("NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration")
	case strings.Contains(op, "Notification"):
		return "notification", nil
	case strings.Contains(op, "Lifecycle"):
		return "lifecycle", n("NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist")
	case strings.Contains(op, "Encryption"):
		return "encryption", n("ServerSideEncryptionConfigurationNotFoundError", "The server side encryption configuration was not found")
	case strings.Contains(op, "Replication"):
		return "replication", n("ReplicationConfigurationNotFoundError", "The replication configuration was not found")
	case strings.Contains(op, "ObjectLock"):
		return "objectlock", n("ObjectLockConfigurationNotFoundError", "Object Lock configuration does not exist")
	case strings.Contains(op, "Abac"):
		return "abac", n("NoSuchAbacConfiguration", "The ABAC configuration does not exist")
	case strings.Contains(op, "Logging"):
		return "logging", nil
	case strings.Contains(op, "RequestPayment"):
		return "requestpayment", nil
	case strings.Contains(op, "Accelerate"):
		return "accelerate", nil
	case strings.Contains(op, "Acl"):
		return "acl", nil
	case strings.Contains(op, "PublicAccessBlock"):
		return "publicaccessblock", n("NoSuchPublicAccessBlockConfiguration", "The public access block configuration was not found")
	case strings.Contains(op, "OwnershipControls"):
		return "ownershipcontrols", n("OwnershipControlsNotFoundError", "The bucket ownership controls were not found")
	}
	return strings.ToLower(op), nil
}

func (p *Pack) policyStatus(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	raw, ok, _ := p.col(req, "bktcfg").Get(ctx, b+"/policy")
	pub := false
	if ok && bytes.Contains(raw, []byte(`"AWS":"*"`)) || ok && bytes.Contains(raw, []byte(`"Principal":"*"`)) {
		pub = true
	}
	return &spi.Response{Output: map[string]any{"PolicyStatus": map[string]any{"IsPublic": pub}}}, nil
}

func (p *Pack) objectAttributes(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	raw, ok, _ := p.col(req, "objects").Get(ctx, b+"/"+key)
	if !ok {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	return &spi.Response{Output: map[string]any{"ETag": meta["etag"], "ObjectSize": meta["size"], "LastModified": meta["mtime"]}}, nil
}

func (p *Pack) emptyOK(req *spi.Request) (*spi.Response, error) {
	ctx := context.Background()
	b := str(req.Input["Bucket"])
	key := str(req.Input["Key"])
	switch req.Operation {
	case "DeleteBucketTagging", "DeleteObjectTagging":
		tagKey := b
		if req.Operation == "DeleteObjectTagging" {
			tagKey = b + "/" + key
		}
		_ = p.col(req, "tags").Delete(ctx, tagKey)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutBucketTagging", "PutObjectTagging":
		tagKey := b
		if req.Operation == "PutObjectTagging" {
			tagKey = b + "/" + key
		}
		raw, _ := json.Marshal(req.Input["TagSet"])
		if len(raw) == 0 || string(raw) == "null" {
			raw = []byte("[]")
		}
		_ = p.col(req, "tags").Put(ctx, tagKey, raw)
		if req.Operation == "PutObjectTagging" {
			p.syncReplicaTags(ctx, req, b, key, raw)
		}
		return &spi.Response{Status: 200, Output: map[string]any{"TagSet": json.RawMessage(raw)}}, nil
	case "GetBucketTagging", "GetObjectTagging":
		tagKey := b
		if req.Operation == "GetObjectTagging" {
			tagKey = b + "/" + key
		}
		raw, ok, _ := p.col(req, "tags").Get(ctx, tagKey)
		if !ok {
			return &spi.Response{Status: 200, Output: map[string]any{"TagSet": []any{}}}, nil
		}
		var tags any
		_ = json.Unmarshal(raw, &tags)
		if tags == nil {
			tags = []any{}
		}
		return &spi.Response{Status: 200, Output: map[string]any{"TagSet": tags}}, nil
	case "PutBucketNotificationConfiguration":
		raw, _ := json.Marshal(req.Input)
		_ = p.col(req, "notify").Put(ctx, b, raw)
		return &spi.Response{Status: 200, Output: map[string]any{}}, nil
	case "GetBucketNotificationConfiguration":
		raw, ok, _ := p.col(req, "notify").Get(ctx, b)
		if !ok {
			return &spi.Response{Status: 200, Output: map[string]any{}}, nil
		}
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		return &spi.Response{Status: 200, Output: m}, nil
	}
	// Terraform refresh reads: empty document is the documented "not configured" response.
	return &spi.Response{Status: 200, Output: map[string]any{}}, nil
}

func (p *Pack) objectLockExtras(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	kind := "legalhold"
	if strings.Contains(req.Operation, "Retention") {
		kind = "retention"
	}
	if strings.Contains(req.Operation, "Restore") {
		kind = "restore"
	}
	ck := b + "/" + key + "/" + kind
	if strings.HasPrefix(req.Operation, "Put") || req.Operation == "RestoreObject" {
		raw, _ := json.Marshal(req.Input)
		_ = p.col(req, "objlock").Put(ctx, ck, raw)
		p.syncReplicaObjectLock(ctx, req, b, key, kind, raw)
		return &spi.Response{Status: 200, Output: map[string]any{}}, nil
	}
	raw, ok, _ := p.col(req, "objlock").Get(ctx, ck)
	if !ok {
		return &spi.Response{Status: 200, Output: map[string]any{}}, nil
	}
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	return &spi.Response{Status: 200, Output: doc}, nil
}

func (p *Pack) namedCfg(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	kind := "analytics"
	switch {
	case strings.Contains(req.Operation, "Inventory"):
		kind = "inventory"
	case strings.Contains(req.Operation, "Intelligent"):
		kind = "intelligent"
	case strings.Contains(req.Operation, "Metrics"):
		kind = "metrics"
	}
	id := str(req.Input["Id"])
	if id == "" {
		id = str(req.Input["id"])
	}
	if strings.HasPrefix(req.Operation, "List") {
		kvs, _, _ := p.col(req, "namedcfg").List(ctx, b+"/"+kind+"/", "", 0)
		var items []any
		for _, kv := range kvs {
			var doc map[string]any
			_ = json.Unmarshal(kv.Value, &doc)
			items = append(items, doc)
		}
		return &spi.Response{Output: map[string]any{"List": items}}, nil
	}
	ck := b + "/" + kind + "/" + id
	if strings.HasPrefix(req.Operation, "Put") {
		raw, _ := json.Marshal(req.Input)
		_ = p.col(req, "namedcfg").Put(ctx, ck, raw)
		return &spi.Response{Status: 200, Output: map[string]any{}}, nil
	}
	if strings.HasPrefix(req.Operation, "Delete") {
		_ = p.col(req, "namedcfg").Delete(ctx, ck)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	}
	raw, ok, _ := p.col(req, "namedcfg").Get(ctx, ck)
	if !ok {
		return nil, &spi.Fault{Code: "NoSuchConfiguration", HTTPStatus: 404, Fault: "client"}
	}
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	return &spi.Response{Status: 200, Output: doc}, nil
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
	raw, ok, _ := p.col(req, "notify").Get(ctx, bucket)
	if !ok {
		return
	}
	var cfg map[string]any
	_ = json.Unmarshal(raw, &cfg)
	for _, dest := range append(asSlice(cfg["QueueConfigurations"]), asSlice(cfg["TopicConfigurations"])...) {
		m := asMap(dest)
		arn := str(m["QueueArn"])
		if arn == "" {
			arn = str(m["TopicArn"])
		}
		if arn == "" {
			arn = str(m["Queue"])
		}
		if arn == "" {
			continue
		}
		name := arn
		if i := strings.LastIndex(arn, ":"); i >= 0 {
			name = arn[i+1:]
		}
		if str(m["QueueArn"]) != "" || str(m["Queue"]) != "" || strings.Contains(arn, ":sqs:") {
			rh := p.deps.Rand.Hex(16)
			msg, _ := json.Marshal(map[string]any{"id": rh, "body": string(payload), "handle": rh, "visibleAt": 0, "receiveCount": 0, "seq": 1})
			_ = p.col(req, "msgs:"+name).Put(ctx, rh, msg)
			continue
		}
		_ = p.deps.Bus.Publish(ctx, "sns:"+arn, payload)
	}
}

func blobKey(req *spi.Request, b, k string) string {
	return req.Identity.Account + "/" + req.Identity.Region + "/" + b + "/" + k
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func requestCondition(req *spi.Request, input, header string) string {
	if value := str(req.Input[input]); value != "" {
		return value
	}
	if req.HTTP != nil {
		return req.HTTP.Header.Get(header)
	}
	return ""
}

func parseCopySource(req *spi.Request) (bucket, key, version string, err error) {
	source := str(req.Input["CopySource"])
	if source == "" && req.HTTP != nil {
		source = req.HTTP.Header.Get("x-amz-copy-source")
	}
	path, query, _ := strings.Cut(strings.TrimPrefix(source, "/"), "?")
	path, err = url.PathUnescape(path)
	if err != nil {
		return "", "", "", &spi.Fault{Code: "InvalidArgument", HTTPStatus: 400, Fault: "client"}
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", &spi.Fault{Code: "InvalidArgument", HTTPStatus: 400, Fault: "client"}
	}
	for _, field := range strings.Split(query, "&") {
		name, value, found := strings.Cut(field, "=")
		if found && name == "versionId" {
			version, err = url.PathUnescape(value)
			if err != nil || version == "" {
				return "", "", "", &spi.Fault{Code: "InvalidArgument", HTTPStatus: 400, Fault: "client"}
			}
			break
		}
	}
	return parts[0], parts[1], version, nil
}

func (p *Pack) openCopySource(ctx context.Context, req *spi.Request) (*copySource, error) {
	bucket, key, version, err := parseCopySource(req)
	if err != nil {
		return nil, err
	}
	blob := blobKey(req, bucket, key)
	if version != "" {
		blob += "@" + version
	}
	meta, exists := p.objectMetadata(ctx, req, bucket, key, version)
	if !exists {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		if version != "" {
			return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: 400, Fault: "client"}
		}
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	body, info, err := p.deps.Blobs.Get(ctx, blob)
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	return &copySource{bucket: bucket, key: key, version: version, body: body, info: info, meta: meta}, nil
}

func (p *Pack) objectMetadata(ctx context.Context, req *spi.Request, bucket, key, version string) (map[string]any, bool) {
	collection, metaKey := "objects", bucket+"/"+key
	if version != "" {
		collection, metaKey = "versions", metaKey+"/"+version
	}
	raw, exists, _ := p.col(req, collection).Get(ctx, metaKey)
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	return meta, exists
}

func deleteMarkerReadFault(meta map[string]any, explicit bool) *spi.Fault {
	headers := http.Header{}
	headers.Set("x-amz-delete-marker", "true")
	if version := str(meta["versionId"]); version != "" {
		headers.Set("x-amz-version-id", version)
	}
	if explicit {
		headers.Set("Last-Modified", str(meta["mtime"]))
		return &spi.Fault{Code: "MethodNotAllowed", HTTPStatus: http.StatusMethodNotAllowed, Fault: "client", Headers: headers}
	}
	return &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client", Headers: headers}
}

func applyCopySourceRange(body []byte, value string) ([]byte, error) {
	raw, ok := strings.CutPrefix(value, "bytes=")
	startRaw, endRaw, found := strings.Cut(raw, "-")
	start, startErr := strconv.Atoi(startRaw)
	end, endErr := strconv.Atoi(endRaw)
	if !ok || !found || startErr != nil || endErr != nil || start < 0 || end < start {
		return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: 400, Fault: "client"}
	}
	if len(body) <= 5<<20 {
		return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: 400, Fault: "client"}
	}
	if start >= len(body) {
		return nil, &spi.Fault{Code: "InvalidRange", HTTPStatus: 416, Fault: "client"}
	}
	if end >= len(body) {
		end = len(body) - 1
	}
	return body[start : end+1], nil
}

func objectETag(meta map[string]any, md5sum string) string {
	if etag := str(meta["etag"]); etag != "" {
		return etag
	}
	return `"` + md5sum + `"`
}

var (
	crc32C    = crc32.MakeTable(crc32.Castagnoli)
	crc64NVME = crc64.MakeTable(0x9a6c9329ac4bc9b5)
	checksums = []struct{ algorithm, input, header string }{
		{"MD5", "ChecksumMD5", "x-amz-checksum-md5"},
		{"CRC32", "ChecksumCRC32", "x-amz-checksum-crc32"},
		{"CRC32C", "ChecksumCRC32C", "x-amz-checksum-crc32c"},
		{"CRC64NVME", "ChecksumCRC64NVME", "x-amz-checksum-crc64nvme"},
		{"SHA1", "ChecksumSHA1", "x-amz-checksum-sha1"},
		{"SHA256", "ChecksumSHA256", "x-amz-checksum-sha256"},
		{"SHA512", "ChecksumSHA512", "x-amz-checksum-sha512"},
	}
)

func checksumByAlgorithm(algorithm string) (struct{ algorithm, input, header string }, bool) {
	for _, checksum := range checksums {
		if checksum.algorithm == algorithm {
			return checksum, true
		}
	}
	return struct{ algorithm, input, header string }{}, false
}

func validateMultipartChecksumContract(req *spi.Request, algorithm, checksumType string) error {
	if strings.HasPrefix(algorithm, "XXHASH") {
		return spi.NotImplemented("aws.s3", req.Operation+".ChecksumAlgorithm."+algorithm, "emulate")
	}
	if _, ok := checksumByAlgorithm(algorithm); !ok || (checksumType != "COMPOSITE" && checksumType != "FULL_OBJECT") {
		return &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if checksumType == "FULL_OBJECT" && algorithm != "CRC64NVME" && algorithm != "CRC32" && algorithm != "CRC32C" {
		return &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if checksumType == "COMPOSITE" && algorithm == "CRC64NVME" {
		return &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	return nil
}

func validateMultipartPartChecksum(req *spi.Request, selected struct{ algorithm, input, header string }, body []byte) error {
	for _, unsupported := range []struct{ input, header string }{
		{"ChecksumXXHASH64", "x-amz-checksum-xxhash64"},
		{"ChecksumXXHASH3", "x-amz-checksum-xxhash3"},
		{"ChecksumXXHASH128", "x-amz-checksum-xxhash128"},
	} {
		if requestCondition(req, unsupported.input, unsupported.header) != "" {
			return &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
	}
	for _, checksum := range checksums {
		if value := requestCondition(req, checksum.input, checksum.header); value != "" {
			if checksum.algorithm != selected.algorithm {
				return &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
			if value != checksumValue(checksum.input, body) {
				return &spi.Fault{Code: "BadDigest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
		}
	}
	return nil
}

func checksumValue(input string, body []byte) string {
	var sum []byte
	switch input {
	case "ChecksumMD5":
		value := md5.Sum(body)
		sum = value[:]
	case "ChecksumCRC32":
		sum = make([]byte, 4)
		binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	case "ChecksumCRC32C":
		sum = make([]byte, 4)
		binary.BigEndian.PutUint32(sum, crc32.Checksum(body, crc32C))
	case "ChecksumCRC64NVME":
		sum = make([]byte, 8)
		binary.BigEndian.PutUint64(sum, crc64.Checksum(body, crc64NVME))
	case "ChecksumSHA1":
		value := sha1.Sum(body)
		sum = value[:]
	case "ChecksumSHA256":
		value := sha256.Sum256(body)
		sum = value[:]
	case "ChecksumSHA512":
		value := sha512.Sum512(body)
		sum = value[:]
	}
	return base64.StdEncoding.EncodeToString(sum)
}

func validateChecksum(req *spi.Request, body []byte) error {
	for _, checksum := range []struct{ input, header string }{
		{"ChecksumXXHASH64", "x-amz-checksum-xxhash64"},
		{"ChecksumXXHASH3", "x-amz-checksum-xxhash3"},
		{"ChecksumXXHASH128", "x-amz-checksum-xxhash128"},
	} {
		if requestCondition(req, checksum.input, checksum.header) != "" {
			return spi.NotImplemented("aws.s3", req.Operation+"."+checksum.input, "emulate")
		}
	}
	for _, checksum := range append([]struct{ algorithm, input, header string }{{"MD5", "ContentMD5", "Content-MD5"}}, checksums...) {
		if value := requestCondition(req, checksum.input, checksum.header); value != "" {
			input := checksum.input
			if input == "ContentMD5" {
				input = "ChecksumMD5"
			}
			if value != checksumValue(input, body) {
				return &spi.Fault{Code: "BadDigest", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	return nil
}

func providedChecksums(req *spi.Request) map[string]string {
	provided := map[string]string{}
	for _, checksum := range checksums {
		if value := requestCondition(req, checksum.input, checksum.header); value != "" {
			provided[checksum.header] = value
		}
	}
	return provided
}

func setChecksumHeaders(headers http.Header, meta map[string]any) {
	stored := asMap(meta["checksums"])
	for header, value := range stored {
		headers.Set(header, str(value))
	}
	if len(stored) > 0 {
		checksumType := str(meta["checksumType"])
		if checksumType == "" {
			checksumType = "FULL_OBJECT"
		}
		headers.Set("x-amz-checksum-type", checksumType)
	}
}

func etagMatches(condition, etag string) bool {
	for _, value := range strings.Split(condition, ",") {
		value = strings.TrimSpace(value)
		if value == "*" || strings.Trim(value, `"`) == strings.Trim(etag, `"`) {
			return true
		}
	}
	return false
}

func preconditionFailed() error {
	return &spi.Fault{Code: "PreconditionFailed", HTTPStatus: 412, Fault: "client"}
}

func (p *Pack) checkWritePreconditions(ctx context.Context, req *spi.Request, bucket, key string) error {
	match := requestCondition(req, "IfMatch", "If-Match")
	noneMatch := requestCondition(req, "IfNoneMatch", "If-None-Match")
	if match == "" && noneMatch == "" {
		return nil
	}
	raw, exists, _ := p.col(req, "objects").Get(ctx, bucket+"/"+key)
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	exists = exists && !truthy(meta["deleteMarker"])
	etag := str(meta["etag"])
	if match != "" && (!exists || !etagMatches(match, etag)) {
		return preconditionFailed()
	}
	if noneMatch != "" && exists && etagMatches(noneMatch, etag) {
		return preconditionFailed()
	}
	return nil
}

func checkCopySourcePreconditions(req *spi.Request, etag, modified string) error {
	match := requestCondition(req, "CopySourceIfMatch", "x-amz-copy-source-if-match")
	if match != "" {
		if !etagMatches(match, etag) {
			return preconditionFailed()
		}
	} else if value := requestCondition(req, "CopySourceIfUnmodifiedSince", "x-amz-copy-source-if-unmodified-since"); value != "" {
		if condition, err := http.ParseTime(value); err != nil || sourceModifiedAfter(modified, condition) {
			return preconditionFailed()
		}
	}
	noneMatch := requestCondition(req, "CopySourceIfNoneMatch", "x-amz-copy-source-if-none-match")
	if noneMatch != "" {
		if etagMatches(noneMatch, etag) {
			return preconditionFailed()
		}
	} else if value := requestCondition(req, "CopySourceIfModifiedSince", "x-amz-copy-source-if-modified-since"); value != "" {
		if condition, err := http.ParseTime(value); err != nil || !sourceModifiedAfter(modified, condition) {
			return preconditionFailed()
		}
	}
	return nil
}

func sourceModifiedAfter(modified string, condition time.Time) bool {
	mtime, err := http.ParseTime(modified)
	return err == nil && mtime.After(condition)
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "True" || t == "1"
	}
	return false
}

func (p *Pack) versioningEnabled(ctx context.Context, req *spi.Request, b string) bool {
	raw, ok, _ := p.col(req, "versioning").Get(ctx, b)
	return ok && string(raw) == "Enabled"
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
	if value, ok := req.Input["PartNumber"]; ok {
		return asInt(value)
	}
	if value, ok := req.Input["partNumber"]; ok {
		return asInt(value)
	}
	if req.HTTP != nil {
		n, _ := strconv.Atoi(req.HTTP.URL.Query().Get("partNumber"))
		return n
	}
	return 0
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
