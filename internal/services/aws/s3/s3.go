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
	"encoding/xml"
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
	"unicode"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/zeebo/xxh3"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.s3", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements spi.BehaviorPack for S3.
type Pack struct {
	deps      spi.Deps
	mu        sync.Mutex
	versionMu sync.Mutex // ponytail: global lock; use per-object locks if versioned write throughput matters.
	mpu       map[string]*mpu
}

type mpu struct {
	bucket, key, uploadID           string
	storageClass, initiated         string
	tagging                         string
	checksumAlgorithm, checksumType string
	lockDocs                        map[string][]byte
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

const bucketLocationConstraints = "|EU|af-south-1|ap-east-1|ap-east-2|ap-northeast-1|ap-northeast-2|ap-northeast-3|ap-south-1|ap-south-2|ap-southeast-1|ap-southeast-2|ap-southeast-3|ap-southeast-4|ap-southeast-5|ap-southeast-6|ap-southeast-7|ca-central-1|ca-west-1|cn-north-1|cn-northwest-1|eu-central-1|eu-central-2|eu-north-1|eu-south-1|eu-south-2|eu-west-1|eu-west-2|eu-west-3|il-central-1|me-central-1|me-south-1|mx-central-1|sa-east-1|us-east-2|us-gov-east-1|us-gov-west-1|us-west-1|us-west-2|"

func createBucketRegion(endpoint, constraint string) (string, error) {
	illegal := func() error {
		value := constraint
		if value == "" {
			value = "unspecified"
		}
		return &spi.Fault{Code: "IllegalLocationConstraintException", Message: "The " + value + " location constraint is incompatible for the region specific endpoint this request was sent to.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if constraint == "" {
		if endpoint != "us-east-1" {
			return "", illegal()
		}
		return "us-east-1", nil
	}
	if endpoint == "us-east-1" {
		if !strings.Contains(bucketLocationConstraints, "|"+constraint+"|") {
			return "", &spi.Fault{Code: "InvalidLocationConstraint", Message: "The specified location-constraint is not valid", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"LocationConstraint": constraint}}
		}
		if constraint == "EU" {
			return "eu-west-1", nil
		}
		return constraint, nil
	}
	if endpoint == "eu-west-1" {
		if constraint != "EU" && constraint != endpoint {
			return "", illegal()
		}
		return "eu-west-1", nil
	}
	if endpoint != constraint {
		return "", illegal()
	}
	return constraint, nil
}

func validBucketName(name string) bool {
	return validBucketNameRules(name) && !strings.HasSuffix(name, "-an")
}

func validAccountRegionalBucketName(name, account, region string) bool {
	return validBucketNameRules(name) && strings.HasSuffix(name, "-"+account+"-"+region+"-an")
}

func validBucketNameRules(name string) bool {
	if len(name) < 3 || len(name) > 63 || !bucketNameEdge(name[0]) || !bucketNameEdge(name[len(name)-1]) || strings.Contains(name, "..") {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if !bucketNameEdge(c) && c != '.' && c != '-' {
			return false
		}
	}
	if looksLikeIPv4(name) {
		return false
	}
	for _, prefix := range []string{"xn--", "sthree-", "amzn-s3-demo-"} {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	for _, suffix := range []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"} {
		if strings.HasSuffix(name, suffix) {
			return false
		}
	}
	return true
}

func bucketNameEdge(c byte) bool { return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' }

func looksLikeIPv4(name string) bool {
	parts := strings.Split(name, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if len(part) < 1 || len(part) > 3 {
			return false
		}
		for i := range len(part) {
			if part[i] < '0' || part[i] > '9' {
				return false
			}
		}
	}
	return true
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
		"PutObject", "PostObject", "GetObject", "HeadObject", "DeleteObject", "DeleteObjects", "CopyObject",
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
		return p.putObject(ctx, req, "", "", nil, nil)
	case "PostObject":
		return p.postObject(ctx, req)
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
		return p.emptyOK(ctx, req)
	case "PutObjectLegalHold", "GetObjectLegalHold", "PutObjectRetention", "GetObjectRetention":
		return p.objectLockExtras(ctx, req)
	case "RestoreObject":
		return p.restoreObject(ctx, req)
	case "PutBucketAnalyticsConfiguration", "GetBucketAnalyticsConfiguration", "DeleteBucketAnalyticsConfiguration", "ListBucketAnalyticsConfigurations",
		"PutBucketInventoryConfiguration", "GetBucketInventoryConfiguration", "DeleteBucketInventoryConfiguration", "ListBucketInventoryConfigurations",
		"PutBucketMetricsConfiguration", "GetBucketMetricsConfiguration", "DeleteBucketMetricsConfiguration", "ListBucketMetricsConfigurations",
		"PutBucketIntelligentTieringConfiguration", "GetBucketIntelligentTieringConfiguration", "DeleteBucketIntelligentTieringConfiguration", "ListBucketIntelligentTieringConfigurations":
		return p.namedCfg(ctx, req)
	case "ListParts":
		return p.listParts(ctx, req)
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
	if has("max-buckets") {
		req.Input["MaxBuckets"] = q.Get("max-buckets")
	}
	if has("bucket-region") {
		req.Input["BucketRegion"] = q.Get("bucket-region")
	}
	if has("prefix") {
		req.Input["Prefix"] = q.Get("prefix")
	}
	if v := q.Get("versionId"); v != "" {
		req.Input["VersionId"] = v
	}
	if v := q.Get("max-keys"); v != "" {
		req.Input["MaxKeys"] = v
	}
	if has("continuation-token") {
		req.Input["ContinuationToken"] = q.Get("continuation-token")
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
	case m == http.MethodPost && key == "":
		return "PostObject"
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
	objectLock := truthy(req.Input["ObjectLockEnabledForBucket"])
	if req.HTTP != nil {
		objectLock = objectLock || strings.EqualFold(req.HTTP.Header.Get("x-amz-bucket-object-lock-enabled"), "true")
	}
	namespace := requestCondition(req, "BucketNamespace", "x-amz-bucket-namespace")
	if namespace != "" && namespace != "global" && namespace != "account-regional" {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid bucket namespace", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-bucket-namespace", "ArgumentValue": namespace}}
	}
	accountRegional := namespace == "account-regional"
	if !accountRegional && !validBucketName(b) {
		return nil, &spi.Fault{Code: "InvalidBucketName", Message: "The specified bucket is not valid.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"BucketName": b}}
	}
	constraint := str(req.Input["LocationConstraint"])
	if constraint == "" {
		constraint = str(asMap(req.Input["CreateBucketConfiguration"])["LocationConstraint"])
	}
	bucketRegion, err := createBucketRegion(req.Identity.Region, constraint)
	if err != nil {
		return nil, err
	}
	if accountRegional && (bucketRegion == "me-central-1" || bucketRegion == "me-south-1") {
		return nil, &spi.Fault{Code: "InvalidRequest", Message: "Account regional namespace is not supported in this region.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if accountRegional && !validAccountRegionalBucketName(b, req.Identity.Account, bucketRegion) {
		return nil, &spi.Fault{Code: "InvalidBucketName", Message: "The specified bucket is not valid.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"BucketName": b}}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	buckets := p.deps.Store.Scope(req.Identity.Account, bucketRegion).Collection("buckets")
	if accountRegional {
		if _, exists, err := buckets.Get(ctx, b); err != nil {
			return nil, err
		} else if exists {
			return nil, &spi.Fault{Code: "BucketAlreadyOwnedByYou", Message: "Your previous request to create the named bucket succeeded and you already own it.", HTTPStatus: http.StatusConflict, Fault: "client", Fields: map[string]any{"BucketName": b}}
		}
		meta, _ := json.Marshal(map[string]any{"name": b, "region": bucketRegion, "locationConstraint": constraint, "namespace": namespace, "objectLockEnabled": objectLock, "creationDate": p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")})
		if err := buckets.Put(ctx, b, meta); err != nil {
			return nil, err
		}
		if objectLock {
			_ = p.deps.Store.Scope(req.Identity.Account, bucketRegion).Collection("versioning").Put(ctx, b, []byte("Enabled"))
		}
		h := http.Header{}
		h.Set("Location", "/"+b)
		return &spi.Response{Status: 200, Headers: h, Output: map[string]any{}}, nil
	}
	global := p.deps.Store.Scope("_mirror", "global").Collection("s3buckets")
	raw, exists, err := global.Get(ctx, b)
	if err != nil {
		return nil, err
	}
	if exists {
		var location struct {
			Account string `json:"account"`
			Region  string `json:"region"`
		}
		if err := json.Unmarshal(raw, &location); err != nil {
			return nil, err
		}
		if location.Account != req.Identity.Account {
			return nil, &spi.Fault{Code: "BucketAlreadyExists", Message: "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Select a different name and try again.", HTTPStatus: http.StatusConflict, Fault: "client", Fields: map[string]any{"BucketName": b}}
		}
		if bucketRegion != "us-east-1" || location.Region != bucketRegion {
			return nil, &spi.Fault{Code: "BucketAlreadyOwnedByYou", Message: "Your previous request to create the named bucket succeeded and you already own it.", HTTPStatus: http.StatusConflict, Fault: "client", Fields: map[string]any{"BucketName": b}}
		}
	} else {
		meta, _ := json.Marshal(map[string]any{"name": b, "region": bucketRegion, "locationConstraint": constraint, "objectLockEnabled": objectLock, "creationDate": p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")})
		if err := buckets.Put(ctx, b, meta); err != nil {
			return nil, err
		}
		location, _ := json.Marshal(map[string]any{"account": req.Identity.Account, "region": bucketRegion})
		if err := global.Put(ctx, b, location); err != nil {
			_ = buckets.Delete(ctx, b)
			return nil, err
		}
		if objectLock {
			_ = p.deps.Store.Scope(req.Identity.Account, bucketRegion).Collection("versioning").Put(ctx, b, []byte("Enabled"))
		}
	}
	h := http.Header{}
	location := "/" + b
	if bucketRegion != "us-east-1" {
		location = "http://" + b + ".s3.amazonaws.com/"
		if req.HTTP != nil {
			scheme := "http"
			if req.HTTP.TLS != nil {
				scheme = "https"
			}
			location = scheme + "://" + req.HTTP.Host + "/" + b + "/"
		}
	}
	h.Set("Location", location)
	return &spi.Response{Status: 200, Headers: h, Output: map[string]any{}}, nil
}

func (p *Pack) deleteBucket(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	objects, _, err := p.col(req, "objects").List(ctx, b+"/", "", 1)
	if err != nil {
		return nil, err
	}
	versions, _, err := p.col(req, "versions").List(ctx, b+"/", "", 1)
	if err != nil {
		return nil, err
	}
	if len(objects) != 0 || len(versions) != 0 {
		message := "The bucket you tried to delete is not empty"
		if _, ok, err := p.col(req, "versioning").Get(ctx, b); err != nil {
			return nil, err
		} else if ok {
			message += ". You must delete all versions in the bucket."
		}
		return nil, &spi.Fault{Code: "BucketNotEmpty", Message: message, HTTPStatus: http.StatusConflict, Fault: "client", Fields: map[string]any{"BucketName": b}}
	}
	for _, name := range []string{"versioning", "tags", "notify"} {
		if err := p.col(req, name).Delete(ctx, b); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"bktcfg", "namedcfg", "objlock", "annots", "replicas", "tags"} {
		col := p.col(req, name)
		kvs, _, err := col.List(ctx, b+"/", "", 0)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			if err := col.Delete(ctx, kv.Key); err != nil {
				return nil, err
			}
		}
	}
	p.mu.Lock()
	for id, upload := range p.mpu {
		if upload.bucket == b {
			delete(p.mpu, id)
		}
	}
	p.mu.Unlock()
	if err := p.col(req, "buckets").Delete(ctx, b); err != nil {
		return nil, err
	}
	if err := p.deps.Store.Scope("_mirror", "global").Collection("s3buckets").Delete(ctx, b); err != nil {
		return nil, err
	}
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) headBucket(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireBucket(ctx, req, str(req.Input["Bucket"])); err != nil {
		return nil, err
	}
	return &spi.Response{Status: 200}, nil
}

func (p *Pack) listBuckets(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	prefix, prefixSet := req.Input["Prefix"].(string)
	region, regionSet := req.Input["BucketRegion"].(string)
	token, tokenSet := req.Input["ContinuationToken"].(string)
	_, maxSet := req.Input["MaxBuckets"]
	maxBuckets := 0
	if maxSet {
		maxBuckets = asInt(req.Input["MaxBuckets"])
		if maxBuckets < 1 || maxBuckets > 10000 {
			return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid max-buckets value", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
	} else if prefixSet || regionSet || tokenSet {
		maxBuckets = 10000
	}
	after := ""
	if tokenSet && token != "" {
		if len(token) > 1024 {
			return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid continuation token", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		decoded, err := base64.URLEncoding.DecodeString(token)
		if err != nil {
			return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid continuation token", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		after = string(decoded)
	}
	type listedBucket struct{ name, region, created string }
	var listed []listedBucket
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return nil, err
	}
	for _, scope := range scopes {
		if scope.Account != req.Identity.Account {
			continue
		}
		kvs, _, err := p.deps.Store.Scope(scope.Account, scope.Region).Collection("buckets").List(ctx, prefix, "", 0)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			var meta map[string]any
			if err := json.Unmarshal(kv.Value, &meta); err != nil {
				return nil, err
			}
			bucketRegion := str(meta["region"])
			if bucketRegion == "" {
				bucketRegion = scope.Region
			}
			if (regionSet && bucketRegion != region) || kv.Key <= after {
				continue
			}
			created := str(meta["creationDate"])
			if created == "" {
				created = p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")
			}
			listed = append(listed, listedBucket{name: kv.Key, region: bucketRegion, created: created})
		}
	}
	sort.Slice(listed, func(i, j int) bool { return listed[i].name < listed[j].name })
	truncated := maxBuckets > 0 && len(listed) > maxBuckets
	if truncated {
		listed = listed[:maxBuckets]
	}
	buckets := make([]any, 0, len(listed))
	paginated := prefixSet || regionSet || tokenSet || maxSet
	for _, bucket := range listed {
		item := map[string]any{"Name": bucket.name, "CreationDate": bucket.created}
		if paginated {
			item["BucketRegion"] = bucket.region
		}
		buckets = append(buckets, item)
	}
	out := map[string]any{"Buckets": buckets, "Owner": map[string]any{"ID": req.Identity.Account, "DisplayName": "mirror"}}
	if prefixSet {
		out["Prefix"] = prefix
	}
	if truncated {
		out["ContinuationToken"] = base64.URLEncoding.EncodeToString([]byte(listed[len(listed)-1].name))
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) getBucketLocation(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	raw, _, err := p.col(req, "buckets").Get(ctx, b)
	if err != nil {
		return nil, err
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"LocationConstraint": str(meta["locationConstraint"])}}, nil
}

func (p *Pack) putObject(ctx context.Context, req *spi.Request, etag, checksumType string, parts []any, lockDocs map[string][]byte) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	storageClass, err := requestStorageClass(req)
	if err != nil {
		return nil, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	if err := p.checkWritePreconditions(ctx, req, b, key); err != nil {
		return nil, err
	}
	if lockDocs == nil {
		lockDocs, err = p.objectLockForWrite(ctx, req, b)
		if err != nil {
			return nil, err
		}
	}
	tags, err := requestTags(req)
	if err != nil {
		return nil, err
	}
	objectMetadata := requestObjectMetadata(req)
	websiteRedirectLocation := requestCondition(req, "WebsiteRedirectLocation", "x-amz-website-redirect-location")
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
	versioned := p.versioningEnabled(ctx, req, b)
	if versioned {
		p.versionMu.Lock()
		defer p.versionMu.Unlock()
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
	vid := ""
	var versionOrder []string
	if versioned {
		vid = p.deps.Rand.Hex(8)
		current, _ := p.objectMetadata(ctx, req, b, key, "")
		versionOrder = append(p.objectVersionOrder(ctx, req, b, key, current), vid)
		_, _ = p.deps.Blobs.Put(ctx, blobKey(req, b, key)+"@"+vid, bytes.NewReader(body))
		versionMeta := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "versionId": vid, "versionOrder": versionOrder, "mtime": mtime, "key": key, "storageClass": storageClass, "objectMetadata": objectMetadata, "websiteRedirectLocation": websiteRedirectLocation}
		if len(parts) > 0 {
			versionMeta["parts"] = parts
		}
		if len(provided) > 0 {
			versionMeta["checksums"] = provided
			versionMeta["checksumType"] = checksumType
		}
		vm, _ := json.Marshal(versionMeta)
		_ = p.col(req, "versions").Put(ctx, b+"/"+key+"/"+vid, vm)
	}
	metaDoc := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "mtime": mtime, "versionId": vid, "deleteMarker": false, "storageClass": storageClass, "objectMetadata": objectMetadata, "websiteRedirectLocation": websiteRedirectLocation}
	if versioned {
		metaDoc["versionOrder"] = versionOrder
	}
	if len(parts) > 0 {
		metaDoc["parts"] = parts
	}
	if len(provided) > 0 {
		metaDoc["checksums"] = provided
		metaDoc["checksumType"] = checksumType
	}
	meta, _ := json.Marshal(metaDoc)
	_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
	tagKeys := []string{objectTagKey(b, key, "")}
	if vid != "" {
		tagKeys = append(tagKeys, objectTagKey(b, key, vid))
	}
	if len(tags) > 0 {
		raw, _ := json.Marshal(tagSet(tags))
		for _, tagKey := range tagKeys {
			_ = p.col(req, "tags").Put(ctx, tagKey, raw)
		}
	} else {
		for _, tagKey := range tagKeys {
			_ = p.col(req, "tags").Delete(ctx, tagKey)
		}
	}
	for kind, raw := range lockDocs {
		_ = p.col(req, "objlock").Put(ctx, objectLockKey(b, key, vid, kind), raw)
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
	if status := p.replicateObject(ctx, req, b, key, body, metaDoc, tags); status != "" {
		metaDoc["replicationStatus"] = status
		meta, _ = json.Marshal(metaDoc)
		_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
		if vid != "" {
			_ = p.col(req, "versions").Put(ctx, b+"/"+key+"/"+vid, meta)
		}
		h.Set("x-amz-replication-status", status)
	}
	for kind, raw := range lockDocs {
		p.syncReplicaObjectLock(ctx, req, b, key, vid, kind, raw)
	}
	event := "ObjectCreated:Put"
	switch req.Operation {
	case "CopyObject":
		event = "ObjectCreated:Copy"
	case "CompleteMultipartUpload":
		event = "ObjectCreated:CompleteMultipartUpload"
	case "PostObject":
		event = "ObjectCreated:Post"
	}
	p.notify(ctx, req, b, key, event)
	return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"ETag": etag}}, nil
}

func (p *Pack) postObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bucket := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, bucket); err != nil {
		return nil, err
	}
	if req.HTTP == nil || !strings.Contains(req.HTTP.Header.Get("Content-Type"), "multipart/form-data") {
		return nil, &spi.Fault{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold", HTTPStatus: http.StatusPreconditionFailed, Fault: "client", Fields: map[string]any{"Condition": "Bucket POST must be of the enclosure-type multipart/form-data"}}
	}
	reader, err := req.HTTP.MultipartReader()
	if err != nil {
		return nil, &spi.Fault{Code: "MalformedPOSTRequest", Message: "The body of your POST request is not well-formed multipart/form-data.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	fields := map[string]string{}
	var body []byte
	filename, haveFile := "", false
	for {
		part, partErr := reader.NextPart()
		if partErr == io.EOF {
			break
		}
		if partErr != nil {
			return nil, &spi.Fault{Code: "MalformedPOSTRequest", Message: "The body of your POST request is not well-formed multipart/form-data.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		value, readErr := io.ReadAll(part)
		_ = part.Close()
		if readErr != nil {
			return nil, readErr
		}
		if part.FormName() == "file" {
			body, filename, haveFile = value, part.FileName(), true
		} else if part.FormName() != "" {
			fields[part.FormName()] = string(value)
		}
	}
	key := strings.ReplaceAll(fields["key"], "${filename}", filename)
	if key == "" || !haveFile {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "POST requires both key and file form fields", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if err := validatePostPolicy(fields, bucket, len(body), p.deps.Clock.Now()); err != nil {
		return nil, err
	}
	input := map[string]any{"Bucket": bucket, "Key": key, "ContentType": "binary/octet-stream"}
	if tagging, err := postObjectTagging(fields["tagging"]); err != nil {
		return nil, err
	} else if tagging != "" {
		input["Tagging"] = tagging
	}
	if expires := fields["Expires"]; expires != "" {
		if _, err := time.Parse(http.TimeFormat, expires); err != nil {
			return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid Expires field", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "Expires", "ArgumentValue": expires}}
		}
		input["Expires"] = expires
	}
	if algorithm := strings.ToUpper(fields["x-amz-checksum-algorithm"]); algorithm != "" {
		checksum, ok := checksumByAlgorithm(algorithm)
		if !ok || algorithm == "MD5" || algorithm == "SHA512" || strings.HasPrefix(algorithm, "XXHASH") {
			return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid checksum algorithm", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		value := fields[checksum.header]
		if value == "" {
			value = checksumValue(checksum.input, body)
		} else if value != checksumValue(checksum.input, body) {
			return nil, &spi.Fault{Code: "InvalidRequest", Message: "Value for " + checksum.header + " header is invalid.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		input["ChecksumAlgorithm"], input[checksum.input] = algorithm, value
	}
	for form, member := range map[string]string{
		"Cache-Control":                   "CacheControl",
		"Content-Disposition":             "ContentDisposition",
		"Content-Encoding":                "ContentEncoding",
		"Content-Type":                    "ContentType",
		"x-amz-storage-class":             "StorageClass",
		"x-amz-website-redirect-location": "WebsiteRedirectLocation",
	} {
		if value := fields[form]; value != "" {
			input[member] = value
		}
	}
	metadata := map[string]any{}
	for field, value := range fields {
		if name, ok := strings.CutPrefix(strings.ToLower(field), "x-amz-meta-"); ok {
			metadata[name] = value
		}
	}
	if len(metadata) > 0 {
		input["Metadata"] = metadata
	}
	child := *req
	child.Operation, child.Input = "PostObject", input
	child.Body, child.HTTP = io.NopCloser(bytes.NewReader(body)), nil
	response, err := p.putObject(ctx, &child, "", "", nil, nil)
	if err != nil {
		return nil, err
	}
	headers := response.Headers.Clone()
	scheme := req.HTTP.URL.Scheme
	if scheme == "" {
		scheme = "http"
		if req.HTTP.TLS != nil {
			scheme = "https"
		}
	}
	path := "/" + bucket + "/" + key
	if strings.Contains(req.HTTP.Host, ".s3.") {
		path = "/" + key
	}
	location := (&url.URL{Scheme: scheme, Host: req.HTTP.Host, Path: path}).String()
	headers.Set("Location", location)
	status := http.StatusNoContent
	if value := fields["success_action_status"]; value == "200" || value == "201" {
		status, _ = strconv.Atoi(value)
	}
	if redirect, parseErr := url.Parse(fields["success_action_redirect"]); parseErr == nil && redirect.IsAbs() {
		query := redirect.Query()
		query.Set("bucket", bucket)
		query.Set("key", key)
		query.Set("etag", str(response.Output["ETag"]))
		redirect.RawQuery = query.Encode()
		headers.Set("Location", redirect.String())
		return &spi.Response{Status: http.StatusSeeOther, Headers: headers}, nil
	}
	if status == http.StatusCreated {
		return &spi.Response{Status: status, Headers: headers, Output: map[string]any{"Location": location, "Bucket": bucket, "Key": key, "ETag": response.Output["ETag"]}}, nil
	}
	return &spi.Response{Status: status, Headers: headers}, nil
}

func postObjectTagging(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	var document struct {
		XMLName xml.Name
		Tags    []struct {
			Key   *string `xml:"Key"`
			Value *string `xml:"Value"`
		} `xml:"TagSet>Tag"`
	}
	if err := xml.Unmarshal([]byte(value), &document); err != nil {
		return "", &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if document.XMLName.Local != "Tagging" || len(document.Tags) == 0 {
		return "", nil
	}
	tags := url.Values{}
	for _, tag := range document.Tags {
		if tag.Key == nil || tag.Value == nil {
			return "", &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		tags.Set(*tag.Key, *tag.Value)
	}
	return tags.Encode(), nil
}

func validatePostPolicy(fields map[string]string, bucket string, size int, now time.Time) error {
	form := make(map[string]string, len(fields))
	for key, value := range fields {
		form[strings.ToLower(key)] = value
	}
	policy := form["policy"]
	if policy == "" {
		return nil
	}
	complete := func(required []string) (bool, error) {
		found := false
		for _, field := range required {
			if _, ok := form[field]; ok {
				found = true
			}
		}
		if !found {
			return false, nil
		}
		for _, field := range required {
			if _, ok := form[field]; !ok {
				name := http.CanonicalHeaderKey(field)
				if field == "awsaccesskeyid" {
					name = "AWSAccessKeyId"
				}
				return false, &spi.Fault{Code: "InvalidArgument", Message: "Bucket POST must contain a field named '" + name + "'. If it is specified, please check the order of the fields.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": name, "ArgumentValue": ""}}
			}
		}
		return true, nil
	}
	v4, err := complete([]string{"x-amz-signature", "x-amz-algorithm", "x-amz-credential", "x-amz-date"})
	if err != nil {
		return err
	}
	v2, err := complete([]string{"signature", "awsaccesskeyid"})
	if err != nil {
		return err
	}
	if !v2 && !v4 {
		return &spi.Fault{Code: "AccessDenied", Message: "Access Denied", HTTPStatus: http.StatusForbidden, Fault: "client"}
	}
	decoded, err := base64.StdEncoding.DecodeString(policy)
	var document struct {
		Expiration string `json:"expiration"`
		Conditions []any  `json:"conditions"`
	}
	if err != nil || json.Unmarshal(decoded, &document) != nil {
		return &spi.Fault{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", HTTPStatus: http.StatusForbidden, Fault: "client"}
	}
	if document.Expiration != "" {
		expires, parseErr := time.Parse(time.RFC3339Nano, document.Expiration)
		if parseErr != nil {
			return &spi.Fault{Code: "InvalidPolicyDocument", Message: "Invalid Policy: invalid expiration", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		if now.After(expires) {
			return &spi.Fault{Code: "AccessDenied", Message: "Invalid according to Policy: Policy expired.", HTTPStatus: http.StatusForbidden, Fault: "client"}
		}
	}
	for _, condition := range document.Conditions {
		ok, conditionErr := verifyPostPolicyCondition(condition, form, bucket, size)
		if conditionErr != nil {
			return conditionErr
		}
		if !ok {
			raw, _ := json.Marshal(condition)
			return &spi.Fault{Code: "AccessDenied", Message: "Invalid according to Policy: Policy Condition failed: " + string(raw), HTTPStatus: http.StatusForbidden, Fault: "client"}
		}
	}
	return nil
}

func verifyPostPolicyCondition(condition any, form map[string]string, bucket string, size int) (bool, error) {
	switch value := condition.(type) {
	case map[string]any:
		if len(value) > 1 {
			return false, &spi.Fault{Code: "InvalidPolicyDocument", Message: "Invalid Policy: Invalid Simple-Condition: Simple-Conditions must have exactly one property specified.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		for key, expected := range value {
			want, ok := expected.(string)
			if !ok {
				return false, nil
			}
			if strings.EqualFold(key, "bucket") {
				return bucket == want, nil
			}
			return form[strings.ToLower(key)] == want, nil
		}
	case []any:
		if len(value) == 3 {
			op, _ := value[0].(string)
			if op == "content-length-range" {
				minimum, minErr := strconv.ParseInt(fmt.Sprint(value[1]), 10, 64)
				maximum, maxErr := strconv.ParseInt(fmt.Sprint(value[2]), 10, 64)
				if minErr != nil || maxErr != nil {
					return false, nil
				}
				if int64(size) < minimum {
					return false, &spi.Fault{Code: "EntityTooSmall", Message: "Your proposed upload is smaller than the minimum allowed size", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ProposedSize": size, "MinSizeAllowed": minimum}}
				}
				if int64(size) > maximum {
					return false, &spi.Fault{Code: "EntityTooLarge", Message: "Your proposed upload exceeds the maximum allowed size", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ProposedSize": size, "MaxSizeAllowed": maximum}}
				}
				return true, nil
			}
			field, fieldOK := value[1].(string)
			expected, expectedOK := value[2].(string)
			if !fieldOK || !expectedOK || !strings.HasPrefix(field, "$") {
				return false, nil
			}
			actual := form[strings.ToLower(strings.TrimPrefix(field, "$"))]
			if strings.EqualFold(field, "$bucket") {
				actual = bucket
			}
			switch op {
			case "eq":
				return actual == expected, nil
			case "starts-with":
				return strings.HasPrefix(actual, expected), nil
			}
		}
	}
	return false, nil
}

func (p *Pack) getObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	wantVer := str(req.Input["VersionId"])
	meta, exists := p.objectMetadata(ctx, req, b, key, wantVer)
	if !exists {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return nil, deleteMarkerReadFault(meta, wantVer != "")
	}
	if err := p.requireRestored(ctx, req, b, key, meta); err != nil {
		return nil, err
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
	h.Set("Accept-Ranges", "bytes")
	setObjectMetadataHeaders(h, meta)
	if restore, ok := p.restoreState(ctx, req, b, key, meta); ok {
		h.Set("x-amz-restore", restore)
	}
	h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	mtime := str(meta["mtime"])
	if mtime == "" {
		mtime = p.deps.Clock.Now().UTC().Format(http.TimeFormat)
	}
	h.Set("Last-Modified", mtime)
	if vid := str(meta["versionId"]); vid != "" {
		h.Set("x-amz-version-id", vid)
	}
	if count := len(p.storedTags(ctx, req, b, key, wantVer)); count > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(count))
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
	notModified, conditionErr := checkReadPreconditions(req, etag, mtime)
	if conditionErr != nil || notModified {
		_ = rc.Close()
		if conditionErr != nil {
			return nil, conditionErr
		}
		return &spi.Response{Status: http.StatusNotModified, Headers: h}, nil
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	start, length, count, requested, err := objectPartRange(req, meta, int64(len(data)))
	if err != nil {
		return nil, err
	}
	if requested {
		data = data[start : start+length]
		h.Set("Content-Length", strconv.FormatInt(length, 10))
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, info.Size))
		if count > 0 {
			h.Set("x-amz-mp-parts-count", strconv.Itoa(count))
		}
		if requestCondition(req, "ChecksumMode", "x-amz-checksum-mode") == "ENABLED" {
			setPartChecksumHeaders(h, meta, partMetadata(meta, partNumber(req)), length == info.Size)
		}
	}
	if rng := requestCondition(req, "Range", "Range"); rng != "" {
		rangeStart, rangeLength, ranged, err := objectByteRange(rng, int64(len(data)))
		if err != nil {
			return nil, err
		}
		if ranged {
			data = data[rangeStart : rangeStart+rangeLength]
			h.Set("Content-Length", strconv.FormatInt(rangeLength, 10))
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeStart+rangeLength-1, info.Size))
			setPartChecksumHeaders(h, meta, nil, rangeLength == info.Size)
			return &spi.Response{Status: http.StatusPartialContent, Headers: h, Stream: io.NopCloser(bytes.NewReader(data))}, nil
		}
	}
	if requested {
		return &spi.Response{Status: 206, Headers: h, Stream: io.NopCloser(bytes.NewReader(data))}, nil
	}
	return &spi.Response{Status: 200, Headers: h, Stream: io.NopCloser(bytes.NewReader(data))}, nil
}

func (p *Pack) headObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
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
	h.Set("Accept-Ranges", "bytes")
	setObjectMetadataHeaders(h, meta)
	if restore, ok := p.restoreState(ctx, req, b, key, meta); ok {
		h.Set("x-amz-restore", restore)
	}
	h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	h.Set("Last-Modified", str(meta["mtime"]))
	if version := str(meta["versionId"]); version != "" {
		h.Set("x-amz-version-id", version)
	}
	if count := len(p.storedTags(ctx, req, b, key, wantVer)); count > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(count))
	}
	if requestCondition(req, "ChecksumMode", "x-amz-checksum-mode") == "ENABLED" {
		setChecksumHeaders(h, meta)
	}
	setReplicationHeaders(h, meta)
	if notModified, err := checkReadPreconditions(req, h.Get("ETag"), h.Get("Last-Modified")); err != nil {
		return nil, err
	} else if notModified {
		return &spi.Response{Status: http.StatusNotModified, Headers: h}, nil
	}
	start, length, count, requested, err := objectPartRange(req, meta, info.Size)
	if err != nil {
		return nil, err
	}
	status := 200
	if requested {
		status = 206
		h.Set("Content-Length", strconv.FormatInt(length, 10))
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, info.Size))
		if count > 0 {
			h.Set("x-amz-mp-parts-count", strconv.Itoa(count))
		}
		if requestCondition(req, "ChecksumMode", "x-amz-checksum-mode") == "ENABLED" {
			setPartChecksumHeaders(h, meta, partMetadata(meta, partNumber(req)), length == info.Size)
		}
	}
	if rng := requestCondition(req, "Range", "Range"); rng != "" {
		rangeStart, rangeLength, ranged, err := objectByteRange(rng, info.Size)
		if err != nil {
			return nil, err
		}
		if ranged {
			status = http.StatusPartialContent
			h.Set("Content-Length", strconv.FormatInt(rangeLength, 10))
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rangeStart, rangeStart+rangeLength-1, info.Size))
			setPartChecksumHeaders(h, meta, nil, rangeLength == info.Size)
		}
	}
	return &spi.Response{Status: status, Headers: h}, nil
}

func (p *Pack) deleteObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	if bypassSet, _ := governanceBypass(req); bypassSet && !p.bucketObjectLockEnabled(ctx, req, b) {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "x-amz-bypass-governance-retention is only applicable to Object Lock enabled buckets.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-bypass-governance-retention"}}
	}
	wantVer := str(req.Input["VersionId"])
	versioned := p.versioningEnabled(ctx, req, b)
	if !versioned && wantVer == "null" {
		wantVer = ""
	}
	if versioned || wantVer != "" {
		p.versionMu.Lock()
		defer p.versionMu.Unlock()
	}
	if versioned && wantVer == "" {
		vid := p.deps.Rand.Hex(8)
		mtime := p.deps.Clock.Now().UTC().Format(http.TimeFormat)
		current, _ := p.objectMetadata(ctx, req, b, key, "")
		meta, _ := json.Marshal(map[string]any{"deleteMarker": true, "versionId": vid, "versionOrder": append(p.objectVersionOrder(ctx, req, b, key, current), vid), "mtime": mtime, "key": key})
		_ = p.col(req, "objects").Put(ctx, b+"/"+key, meta)
		_ = p.col(req, "versions").Put(ctx, b+"/"+key+"/"+vid, meta)
		h := http.Header{}
		h.Set("x-amz-delete-marker", "true")
		h.Set("x-amz-version-id", vid)
		if status := p.replicateDeleteMarker(ctx, req, b, key, meta); status != "" {
			h.Set("x-amz-replication-status", status)
		}
		_ = p.col(req, "tags").Delete(ctx, objectTagKey(b, key, ""))
		return &spi.Response{Status: 204, Headers: h}, nil
	}
	if wantVer != "" {
		meta, exists := p.objectMetadata(ctx, req, b, key, wantVer)
		if !exists {
			return nil, &spi.Fault{Code: "InvalidArgument", Message: "Invalid version id specified", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "versionId", "ArgumentValue": wantVer}}
		}
		if p.objectVersionLocked(ctx, req, b, key, wantVer) {
			return nil, &spi.Fault{Code: "AccessDenied", Message: "Access Denied", HTTPStatus: http.StatusForbidden, Fault: "client"}
		}
		current, currentExists := p.objectMetadata(ctx, req, b, key, "")
		_ = p.deps.Blobs.Delete(ctx, blobKey(req, b, key)+"@"+wantVer)
		_ = p.col(req, "versions").Delete(ctx, b+"/"+key+"/"+wantVer)
		_ = p.col(req, "tags").Delete(ctx, objectTagKey(b, key, wantVer))
		_ = p.col(req, "objlock").Delete(ctx, objectLockKey(b, key, wantVer, "legalhold"))
		_ = p.col(req, "objlock").Delete(ctx, objectLockKey(b, key, wantVer, "retention"))
		order := p.objectVersionOrder(ctx, req, b, key, current)
		kept := order[:0]
		for _, version := range order {
			if version != wantVer {
				kept = append(kept, version)
			}
		}
		if currentExists && str(current["versionId"]) == wantVer {
			previous := ""
			if len(kept) > 0 {
				previous = kept[len(kept)-1]
			}
			if err := p.restoreCurrentVersion(ctx, req, b, key, previous, kept); err != nil {
				return nil, err
			}
		} else if currentExists {
			current["versionOrder"] = kept
			raw, _ := json.Marshal(current)
			_ = p.col(req, "objects").Put(ctx, b+"/"+key, raw)
		}
		h := http.Header{}
		h.Set("x-amz-version-id", wantVer)
		if truthy(meta["deleteMarker"]) {
			h.Set("x-amz-delete-marker", "true")
		}
		return &spi.Response{Status: 204, Headers: h}, nil
	}
	_ = p.deps.Blobs.Delete(ctx, blobKey(req, b, key))
	_ = p.col(req, "objects").Delete(ctx, b+"/"+key)
	_ = p.col(req, "tags").Delete(ctx, objectTagKey(b, key, ""))
	return &spi.Response{Status: 204}, nil
}

func (p *Pack) deleteObjects(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireBucket(ctx, req, str(req.Input["Bucket"])); err != nil {
		return nil, err
	}
	if req.HTTP != nil {
		checksumPresent := requestCondition(req, "ContentMD5", "Content-MD5") != ""
		for _, checksum := range checksums {
			checksumPresent = checksumPresent || requestCondition(req, checksum.input, checksum.header) != ""
		}
		if !checksumPresent {
			return nil, &spi.Fault{Code: "MissingContentMD5", Message: "Missing required header for this request: Content-MD5", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		if requested := strings.ToUpper(requestCondition(req, "ChecksumAlgorithm", "x-amz-sdk-checksum-algorithm")); requested != "" {
			checksum, ok := checksumByAlgorithm(requested)
			if !ok || requestCondition(req, checksum.input, checksum.header) == "" {
				return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
		}
		if err := validateChecksum(req, []byte(str(req.Input["_body"]))); err != nil {
			return nil, err
		}
	}
	objs, _ := req.Input["Objects"].([]any)
	if objs == nil {
		if d, ok := req.Input["Delete"].(map[string]any); ok {
			objs, _ = d["Objects"].([]any)
		}
	}
	if len(objs) == 0 || len(objs) > 1000 {
		return nil, &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	quiet := truthy(req.Input["Quiet"])
	if d, ok := req.Input["Delete"].(map[string]any); ok {
		quiet = quiet || truthy(d["Quiet"])
	}
	var deleted []any
	var failures []any
	for _, o := range objs {
		m, _ := o.(map[string]any)
		key := str(m["Key"])
		if key == "" {
			continue
		}
		child := *req
		child.Input = cloneMap(req.Input)
		child.Input["Key"] = key
		versionID := str(m["VersionId"])
		if versionID != "" {
			child.Input["VersionId"] = versionID
		} else {
			delete(child.Input, "VersionId")
		}
		resp, err := p.deleteObject(ctx, &child)
		if err != nil {
			fault, ok := err.(*spi.Fault)
			if !ok {
				return nil, err
			}
			code, message := fault.Code, fault.Message
			if versionID != "" && code == "InvalidArgument" {
				code, message = "NoSuchVersion", "The specified version does not exist."
			}
			item := map[string]any{"Key": key, "Code": code, "Message": message}
			if versionID != "" {
				item["VersionId"] = versionID
			}
			failures = append(failures, item)
			continue
		}
		if quiet {
			continue
		}
		item := map[string]any{"Key": key}
		if versionID != "" {
			item["VersionId"] = versionID
		}
		if resp.Headers.Get("x-amz-delete-marker") == "true" {
			item["DeleteMarkerVersionId"] = resp.Headers.Get("x-amz-version-id")
			item["DeleteMarker"] = true
		}
		deleted = append(deleted, item)
	}
	output := map[string]any{}
	if len(deleted) > 0 {
		output["Deleted"] = deleted
	}
	if len(failures) > 0 {
		output["Errors"] = failures
	}
	return &spi.Response{Output: output}, nil
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
	if err := validateObjectKey(str(req.Input["Key"])); err != nil {
		return nil, err
	}
	if err := p.requireBucket(ctx, req, str(req.Input["Bucket"])); err != nil {
		return nil, err
	}
	source, err := p.openCopySource(ctx, req)
	if err != nil {
		return nil, err
	}
	defer source.body.Close()
	if err := checkCopySourcePreconditions(req, objectETag(source.meta, source.info.MD5), str(source.meta["mtime"])); err != nil {
		return nil, err
	}
	_, bucketEncrypted, _ := p.col(req, "bktcfg").Get(ctx, source.bucket+"/encryption")
	_, sourceRestored := p.restoreState(ctx, req, source.bucket, source.key, source.meta)
	if source.bucket == str(req.Input["Bucket"]) && source.key == str(req.Input["Key"]) &&
		requestCondition(req, "StorageClass", "x-amz-storage-class") == "" &&
		requestCondition(req, "ServerSideEncryption", "x-amz-server-side-encryption") == "" &&
		requestCondition(req, "SSECustomerKeyMD5", "x-amz-server-side-encryption-customer-key-MD5") == "" &&
		requestCondition(req, "MetadataDirective", "x-amz-metadata-directive") != "REPLACE" &&
		requestCondition(req, "WebsiteRedirectLocation", "x-amz-website-redirect-location") == "" && !bucketEncrypted && !sourceRestored {
		return nil, &spi.Fault{Code: "InvalidRequest", Message: "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	directive, err := copyDirective(req, "TaggingDirective", "x-amz-tagging-directive")
	if err != nil {
		return nil, err
	}
	if directive != "REPLACE" {
		values := url.Values{}
		for key, value := range p.storedTags(ctx, req, source.bucket, source.key, source.version) {
			values.Set(key, value)
		}
		req.Input["Tagging"] = values.Encode()
	}
	metadataDirective, err := copyDirective(req, "MetadataDirective", "x-amz-metadata-directive")
	if err != nil {
		return nil, err
	}
	if metadataDirective != "REPLACE" {
		req.Input["_ObjectMetadata"] = source.meta["objectMetadata"]
	}
	req.Body = source.body
	response, err := p.putObject(ctx, req, "", "", nil, nil)
	if err == nil && source.version != "" {
		response.Headers.Set("x-amz-copy-source-version-id", source.version)
	}
	return response, err
}

func copyDirective(req *spi.Request, input, header string) (string, error) {
	directive := requestCondition(req, input, header)
	if directive == "" {
		return "COPY", nil
	}
	if directive != "COPY" && directive != "REPLACE" {
		return "", &spi.Fault{Code: "InvalidArgument", Message: "Unknown metadata directive", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": header, "ArgumentValue": directive}}
	}
	return directive, nil
}

func (p *Pack) createMPU(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	storageClass, err := requestStorageClass(req)
	if err != nil {
		return nil, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	if _, err := requestTags(req); err != nil {
		return nil, err
	}
	lockDocs, err := p.objectLockForWrite(ctx, req, b)
	if err != nil {
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
	id := p.deps.Rand.Hex(16)
	p.mu.Lock()
	p.mpu[id] = &mpu{bucket: b, key: key, uploadID: id, storageClass: storageClass, initiated: p.deps.Clock.Now().UTC().Format(time.RFC3339Nano), tagging: requestCondition(req, "Tagging", "x-amz-tagging"), checksumAlgorithm: algorithm, checksumType: checksumType, lockDocs: lockDocs, parts: map[int]multipartPart{}}
	p.mu.Unlock()
	h := http.Header{}
	h.Set("x-amz-checksum-algorithm", algorithm)
	h.Set("x-amz-checksum-type", checksumType)
	return &spi.Response{Headers: h, Output: map[string]any{"Bucket": b, "Key": key, "UploadId": id, "ChecksumAlgorithm": algorithm, "ChecksumType": checksumType}}, nil
}

func (p *Pack) uploadPartCopy(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireMultipartBucket(ctx, req); err != nil {
		return nil, err
	}
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
	if err := p.requireMultipartBucket(ctx, req); err != nil {
		return nil, err
	}
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
	if err := p.requireMultipartBucket(ctx, req); err != nil {
		return nil, err
	}
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
	var completedParts []any
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
		completedParts = append(completedParts, map[string]any{"number": number, "size": len(part.body), "checksums": part.checksums})
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
	req.Input["Bucket"], req.Input["Key"], req.Input["StorageClass"], req.Input["Tagging"] = bucket, key, u.storageClass, u.tagging
	req.Body = io.NopCloser(&buf)
	resp, err := p.putObject(ctx, req, etag, u.checksumType, completedParts, u.lockDocs)
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

func (p *Pack) listParts(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if err := p.requireMultipartBucket(ctx, req); err != nil {
		return nil, err
	}
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
	if err := p.requireMultipartBucket(ctx, req); err != nil {
		return nil, err
	}
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
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	if req.Operation == "PutBucketVersioning" {
		st := str(req.Input["Status"])
		if st == "" {
			return nil, &spi.Fault{Code: "IllegalVersioningConfigurationException", Message: "The Versioning element must be specified", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		if st != "Enabled" && st != "Suspended" {
			return nil, &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		if st == "Suspended" && p.bucketObjectLockEnabled(ctx, req, b) {
			return nil, &spi.Fault{Code: "InvalidBucketState", Message: "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.", HTTPStatus: http.StatusConflict, Fault: "client"}
		}
		_ = p.col(req, "versioning").Put(ctx, b, []byte(st))
		return &spi.Response{Status: 200, Output: map[string]any{}}, nil
	}
	raw, ok, _ := p.col(req, "versioning").Get(ctx, b)
	if !ok || len(raw) == 0 {
		return &spi.Response{Output: map[string]any{}}, nil
	}
	return &spi.Response{Output: map[string]any{"Status": string(raw)}}, nil
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
		if req.Operation == "PutBucketReplication" {
			if !p.versioningEnabled(ctx, req, b) {
				return nil, &spi.Fault{Code: "InvalidRequest", Message: "Versioning must be 'Enabled' on the bucket to apply a replication configuration", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
			if err := p.prepareReplicationConfiguration(ctx, req, req.Input["ReplicationConfiguration"]); err != nil {
				return nil, err
			}
		}
		if req.Operation == "PutBucketObjectLockConfiguration" || req.Operation == "PutObjectLockConfiguration" {
			if !p.versioningEnabled(ctx, req, b) {
				return nil, &spi.Fault{Code: "InvalidBucketState", Message: "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration", HTTPStatus: http.StatusConflict, Fault: "client"}
			}
			if err := validateObjectLockConfiguration(req.Input["ObjectLockConfiguration"]); err != nil {
				return nil, err
			}
			p.setBucketObjectLockEnabled(ctx, req, b)
		}
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
		if (req.Operation == "GetBucketObjectLockConfiguration" || req.Operation == "GetObjectLockConfiguration") && p.bucketObjectLockEnabled(ctx, req, b) {
			return &spi.Response{Output: map[string]any{"ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled"}}}, nil
		}
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

func validateObjectLockConfiguration(value any) error {
	configuration := asMap(value)
	malformed := func() error {
		return &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if str(configuration["ObjectLockEnabled"]) != "Enabled" {
		return malformed()
	}
	rule, exists := configuration["Rule"]
	if !exists {
		return nil
	}
	retention := asMap(asMap(rule)["DefaultRetention"])
	mode := str(retention["Mode"])
	_, days := retention["Days"]
	_, years := retention["Years"]
	if (mode != "GOVERNANCE" && mode != "COMPLIANCE") || days == years || days && asInt(retention["Days"]) < 1 || years && asInt(retention["Years"]) < 1 {
		return malformed()
	}
	return nil
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
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	meta, ok := p.objectMetadata(ctx, req, b, key, str(req.Input["VersionId"]))
	if !ok {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return nil, deleteMarkerReadFault(meta, str(req.Input["VersionId"]) != "")
	}
	requested := map[string]bool{}
	add := func(value string) {
		for _, attr := range strings.Split(value, ",") {
			requested[strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(attr), "_", ""))] = true
		}
	}
	switch attrs := req.Input["ObjectAttributes"].(type) {
	case []any:
		for _, attr := range attrs {
			add(str(attr))
		}
	case []string:
		for _, attr := range attrs {
			add(attr)
		}
	case string:
		add(attrs)
	}
	if req.HTTP != nil {
		for _, value := range req.HTTP.Header.Values("x-amz-object-attributes") {
			add(value)
		}
	}
	out := map[string]any{}
	if requested["ETAG"] {
		out["ETag"] = meta["etag"]
	}
	if requested["OBJECTSIZE"] {
		out["ObjectSize"] = asInt(meta["size"])
	}
	if requested["STORAGECLASS"] && str(meta["storageClass"]) != "STANDARD" {
		out["StorageClass"] = meta["storageClass"]
	}
	parts := asSlice(meta["parts"])
	if requested["CHECKSUM"] && len(asMap(meta["checksums"])) > 0 {
		values := map[string]any{"ChecksumType": meta["checksumType"]}
		for _, checksum := range checksums {
			if value := str(asMap(meta["checksums"])[checksum.header]); value != "" {
				if len(parts) > 0 {
					value = strings.SplitN(value, "-", 2)[0]
				}
				values[checksum.input] = value
			}
		}
		out["Checksum"] = values
	}
	if requested["OBJECTPARTS"] && len(parts) > 0 {
		objectParts := map[string]any{"TotalPartsCount": len(parts)}
		if str(meta["checksumType"]) == "COMPOSITE" {
			readInt := func(input, header string, fallback int) (int, error) {
				value := ""
				if raw, ok := req.Input[input]; ok {
					value = fmt.Sprint(raw)
				} else if req.HTTP != nil {
					value = req.HTTP.Header.Get(header)
				}
				if value == "" {
					return fallback, nil
				}
				return strconv.Atoi(value)
			}
			marker, markerErr := readInt("PartNumberMarker", "x-amz-part-number-marker", 0)
			maxParts, maxErr := readInt("MaxParts", "x-amz-max-parts", 1000)
			if markerErr != nil || maxErr != nil || marker < 0 || maxParts < 0 || maxParts > 1000 {
				return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
			if maxParts == 0 {
				maxParts = 1000
			}
			sort.Slice(parts, func(i, j int) bool { return asInt(asMap(parts[i])["number"]) < asInt(asMap(parts[j])["number"]) })
			var listed []any
			for _, raw := range parts {
				part := asMap(raw)
				number := asInt(part["number"])
				if number <= marker {
					continue
				}
				item := map[string]any{"PartNumber": number, "Size": asInt(part["size"])}
				for _, checksum := range checksums {
					if value := str(asMap(part["checksums"])[checksum.header]); value != "" {
						item[checksum.input] = value
					}
				}
				listed = append(listed, item)
			}
			truncated := len(listed) > maxParts
			if truncated {
				listed = listed[:maxParts]
			}
			objectParts["IsTruncated"], objectParts["MaxParts"], objectParts["PartNumberMarker"] = truncated, maxParts, strconv.Itoa(marker)
			if len(listed) > 0 {
				objectParts["Parts"] = listed
				objectParts["NextPartNumberMarker"] = strconv.Itoa(asInt(asMap(listed[len(listed)-1])["PartNumber"]))
			}
		}
		out["ObjectParts"] = objectParts
	}
	h := http.Header{}
	h.Set("Last-Modified", str(meta["mtime"]))
	if version := str(meta["versionId"]); version != "" {
		h.Set("x-amz-version-id", version)
	}
	if notModified, err := checkReadPreconditions(req, str(meta["etag"]), str(meta["mtime"])); err != nil {
		return nil, err
	} else if notModified {
		return &spi.Response{Status: http.StatusNotModified, Headers: h}, nil
	}
	return &spi.Response{Headers: h, Output: out}, nil
}

func (p *Pack) objectTagTarget(ctx context.Context, req *spi.Request) (string, string, error) {
	b, key, version := str(req.Input["Bucket"]), str(req.Input["Key"]), str(req.Input["VersionId"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return "", "", err
	}
	meta, ok := p.objectMetadata(ctx, req, b, key, version)
	if !ok {
		return "", "", &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return "", "", deleteMarkerReadFault(meta, version != "")
	}
	return objectTagKey(b, key, version), str(meta["versionId"]), nil
}

func (p *Pack) emptyOK(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	key := str(req.Input["Key"])
	switch req.Operation {
	case "PutBucketTagging", "GetBucketTagging", "DeleteBucketTagging":
		if err := p.requireBucket(ctx, req, b); err != nil {
			return nil, err
		}
	}
	switch req.Operation {
	case "DeleteBucketTagging", "DeleteObjectTagging":
		tagKey := b
		objectVersion := ""
		if req.Operation == "DeleteObjectTagging" {
			var err error
			if tagKey, objectVersion, err = p.objectTagTarget(ctx, req); err != nil {
				return nil, err
			}
			if str(req.Input["VersionId"]) == "" && objectVersion != "" {
				_ = p.col(req, "tags").Delete(ctx, objectTagKey(b, key, objectVersion))
			}
		}
		_ = p.col(req, "tags").Delete(ctx, tagKey)
		h := http.Header{}
		if objectVersion != "" {
			h.Set("x-amz-version-id", objectVersion)
		}
		return &spi.Response{Status: 204, Headers: h, Output: map[string]any{}}, nil
	case "PutBucketTagging", "PutObjectTagging":
		tagKey := b
		objectVersion := ""
		if req.Operation == "PutObjectTagging" {
			var err error
			if tagKey, objectVersion, err = p.objectTagTarget(ctx, req); err != nil {
				return nil, err
			}
		}
		limit, kind := 50, "bucket"
		if req.Operation == "PutObjectTagging" {
			limit, kind = 10, "object"
		}
		if err := validateTagSet(req.Input["TagSet"], limit, kind); err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(req.Input["TagSet"])
		if len(raw) == 0 || string(raw) == "null" {
			raw = []byte("[]")
		}
		_ = p.col(req, "tags").Put(ctx, tagKey, raw)
		if req.Operation == "PutObjectTagging" {
			if str(req.Input["VersionId"]) == "" {
				if objectVersion != "" {
					_ = p.col(req, "tags").Put(ctx, objectTagKey(b, key, objectVersion), raw)
				}
				p.syncReplicaTags(ctx, req, b, key, objectVersion, raw)
			}
		}
		h := http.Header{}
		if objectVersion != "" {
			h.Set("x-amz-version-id", objectVersion)
		}
		return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"TagSet": json.RawMessage(raw)}}, nil
	case "GetBucketTagging", "GetObjectTagging":
		tagKey := b
		objectVersion := ""
		if req.Operation == "GetObjectTagging" {
			var err error
			if tagKey, objectVersion, err = p.objectTagTarget(ctx, req); err != nil {
				return nil, err
			}
		}
		raw, ok, _ := p.col(req, "tags").Get(ctx, tagKey)
		h := http.Header{}
		if objectVersion != "" {
			h.Set("x-amz-version-id", objectVersion)
		}
		if !ok {
			if req.Operation == "GetBucketTagging" {
				return nil, &spi.Fault{Code: "NoSuchTagSet", Message: "The TagSet does not exist", HTTPStatus: http.StatusNotFound, Fault: "client"}
			}
			return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"TagSet": []any{}}}, nil
		}
		var tags any
		_ = json.Unmarshal(raw, &tags)
		if tags == nil {
			tags = []any{}
		}
		return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"TagSet": tags}}, nil
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

func validateTagSet(value any, limit int, kind string) error {
	tags, ok := value.([]any)
	if !ok {
		return &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if len(tags) > limit {
		return &spi.Fault{Code: "InvalidTag", Message: "The number of tags exceeds the limit", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	keys := make(map[string]struct{}, len(tags))
	for _, item := range tags {
		tag, ok := item.(map[string]any)
		keyValue, hasKey := tag["Key"]
		valueValue, hasValue := tag["Value"]
		key, keyOK := keyValue.(string)
		tagValue, valueOK := valueValue.(string)
		if !ok || !hasKey || !hasValue || !keyOK || !valueOK || len(tag) != 2 {
			return &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		if _, duplicate := keys[key]; duplicate {
			return &spi.Fault{Code: "InvalidTag", Message: "Cannot provide multiple Tags with the same key", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"TagKey": key}}
		}
		if strings.HasPrefix(key, "aws:") {
			message := "System tags cannot be added/updated by requester"
			if kind == "object" {
				message = "Your TagKey cannot be prefixed with aws:"
			}
			return &spi.Fault{Code: "InvalidTag", Message: message, HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"TagKey": key}}
		}
		if utf8.RuneCountInString(key) < 1 || utf8.RuneCountInString(key) > 128 || !validTagText(key) {
			return &spi.Fault{Code: "InvalidTag", Message: "The TagKey you have provided is invalid", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"TagKey": key}}
		}
		if utf8.RuneCountInString(tagValue) > 256 || !validTagText(tagValue) {
			return &spi.Fault{Code: "InvalidTag", Message: "The TagValue you have provided is invalid", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"TagKey": key, "TagValue": tagValue}}
		}
		keys[key] = struct{}{}
	}
	return nil
}

func validTagText(value string) bool {
	for _, r := range value {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !unicode.IsSpace(r) && !strings.ContainsRune("_.:/=+-@", r) {
			return false
		}
	}
	return true
}

func requestObjectMetadata(req *spi.Request) map[string]any {
	if copied := asMap(req.Input["_ObjectMetadata"]); len(copied) > 0 {
		return cloneMap(copied)
	}
	metadata := map[string]any{}
	for _, field := range []struct{ key, input, header string }{
		{"cacheControl", "CacheControl", "Cache-Control"},
		{"contentDisposition", "ContentDisposition", "Content-Disposition"},
		{"contentEncoding", "ContentEncoding", "Content-Encoding"},
		{"contentLanguage", "ContentLanguage", "Content-Language"},
		{"contentType", "ContentType", "Content-Type"},
		{"expires", "Expires", "Expires"},
	} {
		if value := requestCondition(req, field.input, field.header); value != "" {
			metadata[field.key] = value
		}
	}
	if str(metadata["contentType"]) == "" {
		metadata["contentType"] = "binary/octet-stream"
	}
	user := map[string]any{}
	for key, value := range asMap(req.Input["Metadata"]) {
		user[strings.ToLower(key)] = str(value)
	}
	if req.HTTP != nil {
		for key, values := range req.HTTP.Header {
			if name, ok := strings.CutPrefix(strings.ToLower(key), "x-amz-meta-"); ok && len(values) > 0 {
				user[name] = strings.Join(values, ",")
			}
		}
	}
	if len(user) > 0 {
		metadata["user"] = user
	}
	return metadata
}

func setObjectMetadataHeaders(headers http.Header, meta map[string]any) {
	metadata := asMap(meta["objectMetadata"])
	for _, field := range []struct{ key, header string }{
		{"cacheControl", "Cache-Control"},
		{"contentDisposition", "Content-Disposition"},
		{"contentEncoding", "Content-Encoding"},
		{"contentLanguage", "Content-Language"},
		{"contentType", "Content-Type"},
		{"expires", "Expires"},
	} {
		if value := str(metadata[field.key]); value != "" {
			headers.Set(field.header, value)
		}
	}
	for key, value := range asMap(metadata["user"]) {
		headers.Set("x-amz-meta-"+key, str(value))
	}
	if redirect := str(meta["websiteRedirectLocation"]); redirect != "" {
		headers.Set("x-amz-website-redirect-location", redirect)
	}
}

func (p *Pack) objectLockExtras(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	if !p.bucketObjectLockEnabled(ctx, req, b) {
		return nil, &spi.Fault{Code: "InvalidRequest", Message: "Bucket is missing Object Lock Configuration", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	requestedVersion := str(req.Input["VersionId"])
	meta, ok := p.objectMetadata(ctx, req, b, key, requestedVersion)
	if !ok {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return nil, deleteMarkerReadFault(meta, requestedVersion != "")
	}
	version := str(meta["versionId"])
	if version == "null" {
		version = ""
	}
	kind := "legalhold"
	if strings.Contains(req.Operation, "Retention") {
		kind = "retention"
	}
	ck := objectLockKey(b, key, version, kind)
	if strings.HasPrefix(req.Operation, "Put") {
		raw, _ := json.Marshal(req.Input)
		_ = p.col(req, "objlock").Put(ctx, ck, raw)
		p.syncReplicaObjectLock(ctx, req, b, key, version, kind, raw)
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

func objectLockKey(bucket, key, version, kind string) string {
	return objectTagKey(bucket, key, version) + "/" + kind
}

func (p *Pack) objectLockForWrite(ctx context.Context, req *spi.Request, bucket string) (map[string][]byte, error) {
	mode := requestCondition(req, "ObjectLockMode", "x-amz-object-lock-mode")
	until := requestCondition(req, "ObjectLockRetainUntilDate", "x-amz-object-lock-retain-until-date")
	legal := requestCondition(req, "ObjectLockLegalHoldStatus", "x-amz-object-lock-legal-hold")
	if mode != "" || until != "" || legal != "" {
		if !p.bucketObjectLockEnabled(ctx, req, bucket) {
			return nil, &spi.Fault{Code: "InvalidRequest", Message: "Bucket is missing Object Lock Configuration", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
	}
	if (mode == "") != (until == "") {
		argument := "x-amz-object-lock-retain-until-date"
		if mode == "" {
			argument = "x-amz-object-lock-mode"
		}
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": argument}}
	}
	if mode != "" && mode != "GOVERNANCE" && mode != "COMPLIANCE" {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "Unknown wormMode directive.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-object-lock-mode", "ArgumentValue": mode}}
	}
	if until != "" {
		deadline, err := time.Parse(time.RFC3339, until)
		if err != nil || !p.deps.Clock.Now().Before(deadline) {
			return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-object-lock-retain-until-date", "ArgumentValue": until}}
		}
	}
	if legal != "" && legal != "ON" && legal != "OFF" {
		return nil, &spi.Fault{Code: "InvalidArgument", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	if mode == "" {
		raw, ok, _ := p.col(req, "bktcfg").Get(ctx, bucket+"/objectlock")
		if ok {
			var configuration map[string]any
			_ = json.Unmarshal(raw, &configuration)
			retention := asMap(asMap(asMap(configuration["ObjectLockConfiguration"])["Rule"])["DefaultRetention"])
			mode = str(retention["Mode"])
			if mode != "" {
				now := p.deps.Clock.Now().UTC()
				if days := asInt(retention["Days"]); days != 0 {
					until = now.AddDate(0, 0, days).Format(time.RFC3339)
				} else if years := asInt(retention["Years"]); years != 0 {
					until = now.AddDate(years, 0, 0).Format(time.RFC3339)
				}
			}
		}
	}
	docs := map[string][]byte{}
	if mode != "" && until != "" {
		docs["retention"], _ = json.Marshal(map[string]any{"Retention": map[string]any{"Mode": mode, "RetainUntilDate": until}})
	}
	if legal != "" {
		docs["legalhold"], _ = json.Marshal(map[string]any{"LegalHold": map[string]any{"Status": legal}})
	}
	return docs, nil
}

func (p *Pack) objectVersionLocked(ctx context.Context, req *spi.Request, bucket, key, version string) bool {
	var doc map[string]any
	raw, ok, _ := p.col(req, "objlock").Get(ctx, objectLockKey(bucket, key, version, "legalhold"))
	if ok {
		_ = json.Unmarshal(raw, &doc)
		if strings.EqualFold(str(asMap(doc["LegalHold"])["Status"]), "ON") {
			return true
		}
	}
	raw, ok, _ = p.col(req, "objlock").Get(ctx, objectLockKey(bucket, key, version, "retention"))
	if !ok {
		return false
	}
	doc = nil
	_ = json.Unmarshal(raw, &doc)
	retention := asMap(doc["Retention"])
	until := str(retention["RetainUntilDate"])
	if until == "" {
		return false
	}
	deadline, err := time.Parse(time.RFC3339, until)
	if err == nil && !p.deps.Clock.Now().Before(deadline) {
		return false
	}
	_, bypass := governanceBypass(req)
	return !bypass || !strings.EqualFold(str(retention["Mode"]), "GOVERNANCE")
}

func governanceBypass(req *spi.Request) (bool, bool) {
	if value, ok := req.Input["BypassGovernanceRetention"]; ok {
		return true, truthy(value)
	}
	if req.HTTP != nil {
		if values, ok := req.HTTP.Header[http.CanonicalHeaderKey("x-amz-bypass-governance-retention")]; ok {
			return true, len(values) > 0 && strings.EqualFold(values[0], "true")
		}
	}
	return false, false
}

func (p *Pack) bucketObjectLockEnabled(ctx context.Context, req *spi.Request, bucket string) bool {
	raw, ok, _ := p.col(req, "buckets").Get(ctx, bucket)
	if !ok {
		return false
	}
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	return truthy(meta["objectLockEnabled"])
}

func (p *Pack) setBucketObjectLockEnabled(ctx context.Context, req *spi.Request, bucket string) {
	raw, ok, _ := p.col(req, "buckets").Get(ctx, bucket)
	if !ok {
		return
	}
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	meta["objectLockEnabled"] = true
	raw, _ = json.Marshal(meta)
	_ = p.col(req, "buckets").Put(ctx, bucket, raw)
}

func (p *Pack) restoreObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b, key := str(req.Input["Bucket"]), str(req.Input["Key"])
	if err := p.requireBucket(ctx, req, b); err != nil {
		return nil, err
	}
	version := str(req.Input["VersionId"])
	if version == "" {
		version = str(req.Input["versionId"])
	}
	meta, exists := p.objectMetadata(ctx, req, b, key, version)
	if !exists {
		return nil, &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	if truthy(meta["deleteMarker"]) {
		return nil, deleteMarkerReadFault(meta, version != "")
	}
	storageClass := archiveStorageClass(meta)
	if storageClass == "" {
		return nil, &spi.Fault{Code: "InvalidObjectState", HTTPStatus: http.StatusForbidden, Fault: "client", Fields: map[string]any{"StorageClass": str(meta["storageClass"])}}
	}
	days := asInt(req.Input["Days"])
	if days == 0 {
		days = asInt(asMap(req.Input["RestoreRequest"])["Days"])
	}
	if days == 0 {
		return &spi.Response{Status: http.StatusOK, Output: map[string]any{}}, nil
	}
	_, restored := p.restoreState(ctx, req, b, key, meta)
	now := p.deps.Clock.Now().UTC()
	expires := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, days+1)
	restore := fmt.Sprintf(`ongoing-request="false", expiry-date="%s"`, expires.Format(http.TimeFormat))
	_ = p.col(req, "objlock").Put(ctx, objectRestoreKey(b, key, meta), []byte(restore))
	status := http.StatusAccepted
	if restored {
		status = http.StatusOK
	}
	return &spi.Response{Status: status, Output: map[string]any{}}, nil
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
	expected := requestCondition(req, "ExpectedBucketOwner", "x-amz-expected-bucket-owner")
	return p.requireBucketOwner(ctx, req, b, expected)
}

func (p *Pack) requireMultipartBucket(ctx context.Context, req *spi.Request) error {
	if bucket := str(req.Input["Bucket"]); bucket != "" {
		return p.requireBucket(ctx, req, bucket)
	}
	return nil
}

func (p *Pack) requireBucketOwner(ctx context.Context, req *spi.Request, b, expected string) error {
	if expected != "" {
		if len(expected) != 12 {
			return &spi.Fault{Code: "InvalidBucketOwnerAWSAccountID", Message: "The value of the expected bucket owner parameter must be an AWS Account ID", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		for _, r := range expected {
			if r < '0' || r > '9' {
				return &spi.Fault{Code: "InvalidBucketOwnerAWSAccountID", Message: "The value of the expected bucket owner parameter must be an AWS Account ID", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
		}
	}
	_, ok, _ := p.col(req, "buckets").Get(ctx, b)
	if !ok {
		return &spi.Fault{Code: "NoSuchBucket", Message: "The specified bucket does not exist", HTTPStatus: 404, Fault: "client"}
	}
	if expected != "" && expected != req.Identity.Account {
		return &spi.Fault{Code: "AccessDenied", Message: "Access Denied", HTTPStatus: http.StatusForbidden, Fault: "client"}
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

func requestStorageClass(req *spi.Request) (string, error) {
	storageClass := requestCondition(req, "StorageClass", "x-amz-storage-class")
	if storageClass == "" {
		return "STANDARD", nil
	}
	switch storageClass {
	case "STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "SNOW", "EXPRESS_ONEZONE":
		return storageClass, nil
	default:
		return "", &spi.Fault{Code: "InvalidStorageClass", Message: "The storage class you specified is not valid", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"StorageClassRequested": storageClass}}
	}
}

func validateObjectKey(key string) error {
	if len(key) <= 1024 {
		return nil
	}
	return &spi.Fault{Code: "KeyTooLongError", Message: "Your key is too long", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"MaxSizeAllowed": "1024", "Size": strconv.Itoa(len(key))}}
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
	if err := p.requireBucketOwner(ctx, req, bucket, requestCondition(req, "ExpectedSourceBucketOwner", "x-amz-source-expected-bucket-owner")); err != nil {
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
	if err := p.requireRestored(ctx, req, bucket, key, meta); err != nil {
		return nil, err
	}
	body, info, err := p.deps.Blobs.Get(ctx, blob)
	if err != nil {
		return nil, &spi.Fault{Code: "NoSuchKey", HTTPStatus: 404, Fault: "client"}
	}
	return &copySource{bucket: bucket, key: key, version: version, body: body, info: info, meta: meta}, nil
}

func archiveStorageClass(meta map[string]any) string {
	storageClass := str(meta["storageClass"])
	if storageClass == "GLACIER" || storageClass == "DEEP_ARCHIVE" {
		return storageClass
	}
	return ""
}

func objectRestoreKey(bucket, key string, meta map[string]any) string {
	if version := str(meta["versionId"]); version != "" {
		return bucket + "/" + key + "/" + version + "/restore"
	}
	return bucket + "/" + key + "/restore"
}

func (p *Pack) restoreState(ctx context.Context, req *spi.Request, bucket, key string, meta map[string]any) (string, bool) {
	raw, ok, _ := p.col(req, "objlock").Get(ctx, objectRestoreKey(bucket, key, meta))
	return string(raw), ok
}

func (p *Pack) requireRestored(ctx context.Context, req *spi.Request, bucket, key string, meta map[string]any) error {
	storageClass := archiveStorageClass(meta)
	if storageClass == "" {
		return nil
	}
	if _, ok := p.restoreState(ctx, req, bucket, key, meta); ok {
		return nil
	}
	return &spi.Fault{Code: "InvalidObjectState", Message: "The operation is not valid for the object's storage class", HTTPStatus: http.StatusForbidden, Fault: "client", Fields: map[string]any{"StorageClass": storageClass}}
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

func (p *Pack) objectVersionOrder(ctx context.Context, req *spi.Request, bucket, key string, current map[string]any) []string {
	if raw, recorded := current["versionOrder"].([]any); recorded {
		order := make([]string, 0, len(raw))
		for _, version := range raw {
			order = append(order, str(version))
		}
		return order
	}
	var order []string
	kvs, _, _ := p.col(req, "versions").List(ctx, bucket+"/"+key+"/", "", 0)
	currentID := str(current["versionId"])
	for _, kv := range kvs {
		var meta map[string]any
		_ = json.Unmarshal(kv.Value, &meta)
		if versionID := str(meta["versionId"]); versionID != "" && versionID != currentID {
			order = append(order, versionID)
		}
	}
	if currentID != "" {
		order = append(order, currentID)
	}
	return order
}

func (p *Pack) restoreCurrentVersion(ctx context.Context, req *spi.Request, bucket, key, version string, order []string) error {
	currentKey := bucket + "/" + key
	if version == "" {
		_ = p.col(req, "objects").Delete(ctx, currentKey)
		_ = p.col(req, "tags").Delete(ctx, objectTagKey(bucket, key, ""))
		return p.deps.Blobs.Delete(ctx, blobKey(req, bucket, key))
	}
	raw, exists, _ := p.col(req, "versions").Get(ctx, currentKey+"/"+version)
	if !exists {
		return &spi.Fault{Code: "InternalError", HTTPStatus: http.StatusInternalServerError, Fault: "server"}
	}
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	meta["versionOrder"] = order
	current, _ := json.Marshal(meta)
	_ = p.col(req, "objects").Put(ctx, currentKey, current)
	if truthy(meta["deleteMarker"]) {
		_ = p.deps.Blobs.Delete(ctx, blobKey(req, bucket, key))
	} else {
		body, _, err := p.deps.Blobs.Get(ctx, blobKey(req, bucket, key)+"@"+version)
		if err != nil {
			return err
		}
		defer body.Close()
		if _, err := p.deps.Blobs.Put(ctx, blobKey(req, bucket, key), body); err != nil {
			return err
		}
	}
	tagKey := objectTagKey(bucket, key, "")
	if tags, ok, _ := p.col(req, "tags").Get(ctx, objectTagKey(bucket, key, version)); ok {
		_ = p.col(req, "tags").Put(ctx, tagKey, tags)
	} else {
		_ = p.col(req, "tags").Delete(ctx, tagKey)
	}
	return nil
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
		{"XXHASH64", "ChecksumXXHASH64", "x-amz-checksum-xxhash64"},
		{"XXHASH3", "ChecksumXXHASH3", "x-amz-checksum-xxhash3"},
		{"XXHASH128", "ChecksumXXHASH128", "x-amz-checksum-xxhash128"},
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
	case "ChecksumXXHASH64":
		sum = make([]byte, 8)
		binary.BigEndian.PutUint64(sum, xxhash.Sum64(body))
	case "ChecksumXXHASH3":
		sum = make([]byte, 8)
		binary.BigEndian.PutUint64(sum, xxh3.Hash(body))
	case "ChecksumXXHASH128":
		value := xxh3.Hash128(body).Bytes()
		sum = value[:]
	}
	return base64.StdEncoding.EncodeToString(sum)
}

func validateChecksum(req *spi.Request, body []byte) error {
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

func checkReadPreconditions(req *spi.Request, etag, modified string) (bool, error) {
	if match := requestCondition(req, "IfMatch", "If-Match"); match != "" {
		if !etagMatches(match, etag) {
			return false, preconditionFailed()
		}
	} else if value := requestCondition(req, "IfUnmodifiedSince", "If-Unmodified-Since"); value != "" {
		if condition, err := http.ParseTime(value); err == nil && sourceModifiedAfter(modified, condition) {
			return false, preconditionFailed()
		}
	}
	if noneMatch := requestCondition(req, "IfNoneMatch", "If-None-Match"); noneMatch != "" {
		return etagMatches(noneMatch, etag), nil
	}
	if value := requestCondition(req, "IfModifiedSince", "If-Modified-Since"); value != "" {
		if condition, err := http.ParseTime(value); err == nil && !sourceModifiedAfter(modified, condition) {
			return true, nil
		}
	}
	return false, nil
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

func objectPartRange(req *spi.Request, meta map[string]any, size int64) (start, length int64, count int, requested bool, err error) {
	_, upper := req.Input["PartNumber"]
	_, lower := req.Input["partNumber"]
	requested = upper || lower
	if !requested {
		return 0, size, 0, false, nil
	}
	if requestCondition(req, "Range", "Range") != "" {
		return 0, 0, 0, true, &spi.Fault{Code: "InvalidRequest", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	number := partNumber(req)
	if number < 1 || number > 10000 {
		return 0, 0, 0, true, &spi.Fault{Code: "InvalidPartNumber", HTTPStatus: http.StatusRequestedRangeNotSatisfiable, Fault: "client"}
	}
	parts := asSlice(meta["parts"])
	if len(parts) == 0 {
		if number == 1 {
			return 0, size, 0, true, nil
		}
		return 0, 0, 0, true, &spi.Fault{Code: "InvalidPartNumber", HTTPStatus: http.StatusRequestedRangeNotSatisfiable, Fault: "client"}
	}
	for _, raw := range parts {
		part := asMap(raw)
		partSize := int64(asInt(part["size"]))
		if asInt(part["number"]) == number {
			return start, partSize, len(parts), true, nil
		}
		start += partSize
	}
	return 0, 0, 0, true, &spi.Fault{Code: "InvalidPartNumber", HTTPStatus: http.StatusRequestedRangeNotSatisfiable, Fault: "client"}
}

func partMetadata(meta map[string]any, number int) map[string]any {
	for _, raw := range asSlice(meta["parts"]) {
		part := asMap(raw)
		if asInt(part["number"]) == number {
			return part
		}
	}
	return nil
}

func setPartChecksumHeaders(headers http.Header, meta, part map[string]any, wholeObject bool) {
	if part == nil && wholeObject || str(meta["checksumType"]) != "COMPOSITE" && wholeObject {
		return
	}
	for _, checksum := range checksums {
		headers.Del(checksum.header)
	}
	headers.Del("x-amz-checksum-type")
	if part != nil && str(meta["checksumType"]) == "COMPOSITE" {
		for header, value := range asMap(part["checksums"]) {
			headers.Set(header, str(value))
		}
		headers.Set("x-amz-checksum-type", "COMPOSITE")
	}
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

func objectByteRange(value string, size int64) (start, length int64, requested bool, err error) {
	raw, ok := strings.CutPrefix(value, "bytes=")
	first, last, found := strings.Cut(raw, "-")
	if !ok || !found || strings.Contains(last, ",") || strings.Contains(last, "-") {
		return 0, size, false, nil
	}
	invalid := func() (int64, int64, bool, error) {
		h := http.Header{}
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return 0, 0, true, &spi.Fault{Code: "InvalidRange", Message: "The requested range is not satisfiable", HTTPStatus: http.StatusRequestedRangeNotSatisfiable, Fault: "client", Headers: h}
	}
	if first == "" {
		suffix, parseErr := strconv.ParseInt(last, 10, 64)
		if parseErr != nil || suffix < 0 {
			return 0, size, false, nil
		}
		if suffix == 0 || size == 0 {
			return invalid()
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, suffix, true, nil
	}
	start, parseErr := strconv.ParseInt(first, 10, 64)
	if parseErr != nil || start < 0 {
		return 0, size, false, nil
	}
	if start >= size {
		return invalid()
	}
	end := size - 1
	if last != "" {
		end, parseErr = strconv.ParseInt(last, 10, 64)
		if parseErr != nil || end < start {
			return 0, size, false, nil
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end - start + 1, true, nil
}
