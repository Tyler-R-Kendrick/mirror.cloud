Warning: truncated output (original token count: 61184)
Total output lines: 6308

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
	"mime"
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/identity"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
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
	bucket, key, uploadID             string
	storageClass, initiated           string
	tagging                           string
	checksumAlgorithm, checksumType   string
	serverSideEncryption, sseKMSKeyID string
	sseCustomerKeyMD5                 string
	bucketKeyEnabled                  bool
	precondition                      bool
	objectMetadata                    map[string]any
	websiteRedirectLocation           string
	initiator                         map[string]any
	acl                               map[string]any
	lockDocs                          map[string][]byte
	parts                             map[int]multipartPart
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

func createBucketRegion(endpoint, constraint string, allowNonstandard bool) (string, error) {
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
		if !allowNonstandard && !strings.Contains(bucketLocationConstraints, "|"+constraint+"|") {
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
	if req.HTTP != nil && req.HTTP.Method == http.MethodOptions {
		return p.corsPreflight(ctx, req)
	}
	resp, err := p.invoke(ctx, req)
	if err == nil && resp != nil {
		if resp.Headers == nil {
			resp.Headers = http.Header{}
		}
		p.applyCORS(ctx, req, resp.Headers)
	}
	return resp, err
}

func (p *Pack) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
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
	case "GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration", "DeleteBucketLifecycle":
		return p.bucketLifecycle(ctx, req)
	case "GetBucketAcl", "PutBucketAcl", "GetObjectAcl", "PutObjectAcl",
		"GetBucketPolicy", "PutBucketPolicy", "DeleteBucketPolicy",
		"GetBucketCors", "PutBucketCors", "DeleteBucketCors",
		"GetBucketWebsite", "PutBucketWebsite", "DeleteBucketWebsite",
		"GetBucketLogging", "PutBucketLogging",
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

func (p *Pack) corsPreflight(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	origin := req.HTTP.Header.Get("Origin")
	if origin == "" {
		return nil, &spi.Fault{Code: "BadRequest", Message: "Insufficient information. Origin request header needed.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	method := req.HTTP.Header.Get("Access-Control-Request-Method")
	if method == "" {
		method = http.MethodOptions
	}
	headers, configured := p.corsHeaders(ctx, req, origin, method, req.HTTP.Header.Get("Access-Control-Request-Headers"))
	if headers != nil {
		return &spi.Response{Status: http.StatusOK, Headers: headers}, nil
	}
	message := "CORSResponse: This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource's CORS spec."
	if !configured {
		message = "CORSResponse: CORS is not enabled for this bucket."
	}
	resourceType := "BUCKET"
	if str(req.Input["Key"]) != "" {
		resourceType = "OBJECT"
	}
	return nil, &spi.Fault{Code: "AccessForbidden", Message: message, HTTPStatus: http.StatusForbidden, Fault: "client", Fields: map[string]any{"Method": method, "ResourceType": resourceType}}
}

func (p *Pack) applyCORS(ctx context.Context, req *spi.Request, headers http.Header) {
	if req.HTTP == nil || req.HTTP.Header.Get("Origin") == "" {
		return
	}
	method := req.HTTP.Header.Get("Access-Control-Request-Method")
	if method == "" {
		method = req.HTTP.Method
	}
	matched, _ := p.corsHeaders(ctx, req, req.HTTP.Header.Get("Origin"), method, req.HTTP.Header.Get("Access-Control-Request-Headers"))
	for key, values := range matched {
		for _, value := range values {
			headers.Add(key, value)
		}
	}
}

func (p *Pack) corsHeaders(ctx context.Context, req *spi.Request, origin, method, requested string) (http.Header, bool) {
	configuration, ok := p.corsConfiguration(ctx, req)
	if !ok {
		return localstackCORSHeaders(req.HTTP, origin), false
	}
	requestedHeaders := splitCORSHeaders(requested)
	for _, value := range asSlice(configuration["CORSRules"]) {
		rule := asMap(value)
		allowedOrigin := ""
		for _, candidate := range asSlice(rule["AllowedOrigins"]) {
			pattern := str(candidate)
			if corsPatternMatch(pattern, origin, false) {
				allowedOrigin = pattern
				break
			}
		}
		if allowedOrigin == "" || !containsString(asSlice(rule["AllowedMethods"]), method, false) || !corsHeadersAllowed(asSlice(rule["AllowedHeaders"]), requestedHeaders) {
			continue
		}
		headers := http.Header{}
		headers.Set("Access-Control-Allow-Origin", allowedOrigin)
		if allowedOrigin != "*" {
			headers.Set("Access-Control-Allow-Origin", origin)
			headers.Set("Access-Control-Allow-Credentials", "true")
		}
		headers.Set("Access-Control-Allow-Methods", joinStrings(asSlice(rule["AllowedMethods"])))
		if len(requestedHeaders) > 0 {
			headers.Set("Access-Control-Allow-Headers", strings.Join(requestedHeaders, ", "))
		}
		if exposed := joinStrings(asSlice(rule["ExposeHeaders"])); exposed != "" {
			headers.Set("Access-Control-Expose-Headers", exposed)
		}
		if age, exists := rule["MaxAgeSeconds"]; exists {
			headers.Set("Access-Control-Max-Age", fmt.Sprint(age))
		}
		headers.Set("Vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method")
		return headers, true
	}
	return nil, true
}

func (p *Pack) corsConfiguration(ctx context.Context, req *spi.Request) (map[string]any, bool) {
	bucket := str(req.Input["Bucket"])
	raw, ok, _ := p.col(req, "bktcfg").Get(ctx, bucket+"/cors")
	if !ok {
		location, exists, _ := p.deps.Store.Scope("_mirror", "global").Collection("s3buckets").Get(ctx, bucket)
		if exists {
			var owner struct {
				Account string `json:"account"`
				Region  string `json:"region"`
			}
			if json.Unmarshal(location, &owner) == nil && owner.Account == req.Identity.Account && owner.Region != "" {
				req.Identity.Region = owner.Region
				raw, ok, _ = p.col(req, "bktcfg").Get(ctx, bucket+"/cors")
			}
		}
	}
	if !ok {
		return nil, false
	}
	var doc map[string]any
	if json.Unmarshal(raw, &doc) != nil {
		return nil, false
	}
	return asMap(doc["CORSConfiguration"]), true
}

func splitCORSHeaders(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	headers := parts[:0]
	for _, part := range parts {
		if part = strings.ToLower(strings.TrimSpace(part)); part != "" {
			headers = append(headers, part)
		}
	}
	return headers
}

func localstackCORSHeaders(request *http.Request, origin string) http.Header {
	if !localstackCORSOriginAllowed(request, origin) {
		return nil
	}
	headers := http.Header{}
	headers.Set("Access-Control-Allow-Origin", origin)
	headers.Set("Access-Control-Allow-Credentials", "true")
	headers.Set("Access-Control-Allow-Methods", "HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH")
	headers.Set("Access-Control-Allow-Headers", "authorization,cache-control,content-length,content-md5,content-type,etag,location,x-amz-acl,x-amz-content-sha256,x-amz-date,x-amz-request-id,x-amz-security-token,x-amz-tagging,x-amz-target,x-amz-user-agent,x-amz-version-id,x-amzn-requestid,x-localstack-target,amz-sdk-invocation-id,amz-sdk-request,x-amz-log-type")
	headers.Set("Access-Control-Expose-Headers", "etag,x-amz-version-id,x-amz-log-result,x-amz-executed-version,x-amz-function-error")
	headers.Set("Vary", "Origin")
	if request != nil && request.Header.Get("Access-Control-Request-Private-Network") == "true" {
		headers.Set("Access-Control-Allow-Private-Network", "true")
	}
	return headers
}

func localstackCORSOriginAllowed(request *http.Request, origin string) bool {
	switch origin {
	case "https://app.localstack.cloud", "http://app.localstack.cloud", "https://localhost", "https://localhost.localstack.cloud", "file://":
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || request == nil || parsed.Scheme != "http" && parsed.Scheme != "https" {
		return false
	}
	endpoint, err := url.Parse("//" + request.Host)
	if err != nil {
		return false
	}
	portAllowed := parsed.Port() == "" || parsed.Port() == endpoint.Port()
	for _, marker := range []string{".s3-website.", ".cloudfront."} {
		if _, domain, ok := strings.Cut(parsed.Hostname(), marker); ok && portAllowed && (domain == "localhost" || domain == "localhost.localstack.cloud") {
			return true
		}
	}
	return parsed.Port() != "" && parsed.Port() == endpoint.Port() && (parsed.Hostname() == "localhost" || parsed.Hostname() == "localhost.localstack.cloud")
}

func corsHeadersAllowed(allowed []any, requested []string) bool {
	for _, header := range requested {
		if !containsString(allowed, header, true) {
			return false
		}
	}
	return true
}

func containsString(values []any, target string, fold bool) bool {
	for _, value := range values {
		if corsPatternMatch(str(value), target, fold) {
			return true
		}
	}
	return false
}

func corsPatternMatch(pattern, value string, fold bool) bool {
	if fold {
		pattern, value = strings.ToLower(pattern), strings.ToLower(value)
	}
	prefix, suffix, wildcard := strings.Cut(pattern, "*")
	if !wildcard {
		return pattern == value
	}
	return len(value) >= len(prefix)+len(suffix) && strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix)
}

func joinStrings(values []any) string {
	stringsOut := make([]string, 0, len(values))
	for _, value := range values {
		stringsOut = append(stringsOut, str(value))
	}
	return strings.Join(stringsOut, ", ")
}

func (p *Pack) route(req *spi.Request) string {
	r := req.HTTP
	path := strings.TrimPrefix(r.URL.Path, "/")
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	bucket, key := "", ""
	website := false
	if bucket, website = websiteBucketHost(host); website {
		key = path
	} else if strings.Contains(host, ".s3.") {
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
	if key != "" || website {
		req.Input["Key"] = key
	}
	if website {
		return "GetObject"
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
	if has("delimiter") {
		req.Input["Delimiter"] = q.Get("delimiter")
	}
	if has("marker") {
		req.Input["Marker"] = q.Get("marker")
	}
	if has("key-marker") {
		req.Input["KeyMarker"] = q.Get("key-marker")
	}
	if has("version-id-marker") {
		req.Input["VersionIdMarker"] = q.Get("version-id-marker")
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
		if q.Get("list-type") == "2" {
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
	configuration := asMap(req.Input["CreateBucketConfiguration"])
	tagSet, tagsSet := configuration["Tags"]
	tags := asSlice(tagSet)
	if tagsSet && tagSet != nil {
		if err := validateTagSet(tagSet, 50, "create-bucket"); err != nil {
			return nil, err
		}
	}
	var tagDocument []byte
	if len(tags) > 0 {
		tagDocument, _ = json.Marshal(tags)
	}
	ownership := str(req.Input["ObjectOwnership"])
	_, ownershipSet := req.Input["ObjectOwnership"]
	if !ownershipSet && req.HTTP != nil {
		if values := req.HTTP.Header.Values("x-amz-object-ownership"); len(values) > 0 {
			ownership, ownershipSet = values[0], true
		}
	}
	if !ownershipSet {
		ownership = "BucketOwnerEnforced"
	}
	var ownershipDocument []byte
	validateOwnership := func() error {
		switch ownership {
		case "BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced":
		default:
			return &spi.Fault{Code: "InvalidArgument", Message: "Invalid x-amz-object-ownership header", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-object-ownership", "ArgumentValue": ownership}}
		}
		ownershipDocument, _ = json.Marshal(map[string]any{"OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": ownership}}}})
		return nil
	}
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
		constraint = str(configuration["LocationConstraint"])
	}
	bucketRegion, err := createBucketRegion(req.Identity.Region, constraint, p.deps.S3AllowNonstandardRegions)
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
	bucketStore := p.deps.Store.Scope(req.Identity.Account, bucketRegion)
	buckets := bucketStore.Collection("buckets")
	persistConfigurations := func() error {
		if len(tagDocument) > 0 {
			if err := bucketStore.Collection("tags").Put(ctx, b, tagDocument); err != nil {
				return err
			}
		}
		if err := bucketStore.Collection("bktcfg").Put(ctx, b+"/ownershipcontrols", ownershipDocument); err != nil {
			_ = bucketStore.Collection("tags").Delete(ctx, b)
			return err
		}
		return nil
	}
	if accountRegional {
		if _, exists, err := buckets.Get(ctx, b); err != nil {
			return nil, err
		} else if exists {
			return nil, &spi.Fault{Code: "BucketAlreadyOwnedByYou", Message: "Your previous request to create the named bucket succeeded and you already own it.", HTTPStatus: http.StatusConflict, Fault: "client", Fields: map[string]any{"BucketName": b}}
		}
		if err := validateOwnership(); err != nil {
			return nil, err
		}
		meta, _ := json.Marshal(map[string]any{"name": b, "region": bucketRegion, "locationConstraint": constraint, "namespace": namespace, "objectLockEnabled": objectLock, "creationDate": p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")})
		if err := buckets.Put(ctx, b, meta); err != nil {
			return nil, err
		}
		if err := persistConfigurations(); err != nil {
			_ = buckets.Delete(ctx, b)
			return nil, err
		}
		if objectLock {
			_ = p.deps.Store.Scope(req.Identity.Account, bucketRegion).Collection("versioning").Put(ctx, b, []byte("Enabled"))
		}
		h := http.Header{}
		h.Set("Location", "/"+b)
		h.Set("x-amz-bucket-arn", "arn:aws:s3:::"+b)
		return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"BucketArn": "arn:aws:s3:::" + b}}, nil
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
		if bucketRegion != "us-east-1" || location.Region != bucketRegion || len(tags) > 0 {
			return nil, &spi.Fault{Code: "BucketAlreadyOwnedByYou", Message: "Your previous request to create the named bucket succeeded and you already own it.", HTTPStatus: http.StatusConflict, Fault: "client", Fields: map[string]any{"BucketName": b}}
		}
	} else {
		if err := validateOwnership(); err != nil {
			return nil, err
		}
		meta, _ := json.Marshal(map[string]any{"name": b, "region": bucketRegion, "locationConstraint": constraint, "objectLockEnabled": objectLock, "creationDate": p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")})
		if err := buckets.Put(ctx, b, meta); err != nil {
			return nil, err
		}
		location, _ := json.Marshal(map[string]any{"account": req.Identity.Account, "region": bucketRegion})
		if err := global.Put(ctx, b, location); err != nil {
			_ = buckets.Delete(ctx, b)
			return nil, err
		}
		if err := persistConfigurations(); err != nil {
			_ = global.Delete(ctx, b)
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
	h.Set("x-amz-bucket-arn", "arn:aws:s3:::"+b)
	return &spi.Response{Status: 200, Headers: h, Output: map[string]any{"BucketArn": "arn:aws:s3:::" + b}}, nil
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
	if req.Identity.Region == "aws-global" {
		return nil, &spi.Fault{Code: "AuthorizationHeaderMalformed", Message: "The authorization header is malformed; the region 'aws-global' is wrong; expecting 'us-east-1'", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"Region": "us-east-1"}}
	}
	bucket := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, bucket); err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml")
	headers.Set("x-amz-access-point-alias", "false")
	headers.Set("x-amz-bucket-region", req.Identity.Region)
	headers.Set("x-amz-bucket-arn", "arn:aws:s3:::"+bucket)
	return &spi.Response{Status: 200, Headers: headers}, nil
}

func (p *Pack) listBuckets(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	prefix, prefixSet := req.Input["Prefix"].(string)
	region, regionSet := req.Input["BucketRegion"].(string)
	if regionSet && !p.deps.S3AllowNonstandardRegions && region != "us-east-1" && !strings.Contains(bucketLocationConstraints, "|"+region+"|") {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "Argument value " + region + " is not a valid AWS Region", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "bucket-region"}}
	}
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
		item := map[string]any{"Name": bucket.name, "CreationDate": bucket.created, "BucketArn": "arn:aws:s3:::" + bucket.name}
		if paginated {
			item["BucketRegion"] = bucket.region
		}
		buckets = append(buckets, item)
	}
	out := map[string]any{"Buckets": buckets, "Owner": map[string]any{"ID": req.Identity.Account}}
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
	acl, explicitACL, err := requestACL(req, false)
	if err != nil {
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
	sseCustomerKeyMD5 := str(asMap(req.Input["_ObjectEncryption"])["sseCustomerKeyMD5"])
	if sseCustomerKeyMD5 == "" {
		sseCustomerKeyMD5, err = requestSSECustomerKey(req)
		if err != nil {
			return nil, err
		}
	}
	serverSideEncryption, sseKMSKeyID, bucketKeyEnabled := "", "", false
	if sseCustomerKeyMD5 == "" {
		serverSideEncryption, sseKMSKeyID, bucketKeyEnabled, err = p.objectEncryption(ctx, req, b)
		if err != nil {
			return nil, err
		}
	}
	var body []byte
	if req.Body != nil {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	}
	if checksumType == "" {
		checksumType = "FULL_OBJECT"
	}
	if checksumType == "FULL_OBJECT" {
		if err := validateChecksum(req, body); err != nil {
			return nil, err
		}
	}
	versioningStatus := p.versioningStatus(ctx, req, b)
	versioned := versioningStatus != ""
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
	var deletedVersionNext map[string]any
	if versioned {
		vid = "null"
		if versioningStatus == "Enabled" {
			vid = p.deps.Rand.Hex(32)
		}
		current, _ := p.objectMetadata(ctx, req, b, key, "")
		for _, version := range p.objectVersionOrder(ctx, req, b, key, current) {
			if version != vid {
				versionOrder = append(versionOrder, version)
			}
		}
		versionOrder = append(versionOrder, vid)
		deletedVersionNext = asMap(current["deletedVersionNext"])
		_, _ = p.deps.Blobs.Put(ctx, blobKey(req, b, key)+"@"+vid, bytes.NewReader(body))
		versionMeta := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "versionId": vid, "versionOrder": versionOrder, "mtime": mtime, "key": key, "storageClass": storageClass, "objectMetadata": objectMetadata, "websiteRedirectLocation": websiteRedirectLocation, "serverSideEncryption": serverSideEncryption, "ssekmsKeyId": sseKMSKeyID, "bucketKeyEnabled": bucketKeyEnabled, "sseCustomerKeyMD5": sseCustomerKeyMD5}
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
	metaDoc := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "mtime": mtime, "versionId": vid, "deleteMarker": false, "storageClass": storageClass, "objectMetadata": objectMetadata, "websiteRedirectLocation": websiteRedirectLocation, "serverSideEncryption": serverSideEncryption, "ssekmsKeyId": sseKMSKeyID, "bucketKeyEnabled": bucketKeyEnabled, "sseCustomerKeyMD5": sseCustomerKeyMD5}
	if versioned {
		metaDoc["versionOrder"] = versionOrder
		if len(deletedVersionNext) > 0 {
			metaDoc["deletedVersionNext"] = deletedVersionNext
		}
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
	aclKey := objectTagKey(b, key, vid) + "/acl"
	if explicitACL {
		raw, _ := json.Marshal(acl)
		_ = p.col(req, "bktcfg").Put(ctx, aclKey, raw)
	} else {
		_ = p.col(req, "bktcfg").Delete(ctx, aclKey)
	}
	h := http.Header{}
	h.Set("ETag", etag)
	if versioningStatus == "Enabled" || vid == "null" && (req.Operation == "CopyObject" || req.Operation == "CompleteMultipartUpload") {
		h.Set("x-amz-version-id", vid)
	}
	for header, value := range provided {
		h.Set(header, value)
	}
	if len(provided) > 0 {
		h.Set("x-amz-checksum-type", checksumType)
	}
	setObjectEncryptionHeaders(h, metaDoc)
	if req.Operation == "PutObject" || req.Operation == "PostObject" || req.Operation == "CompleteMultipartUpload" {
		p.setLifecycleExpirationHeader(ctx, req, b, key, info.Size, tags, mtime, h)
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
	p.notify(ctx, req, b, key, event, metaDoc)
	out := map[string]any{"ETag": etag}
	if req.Operation == "CopyObject" {
		modified, _ := http.ParseTime(mtime)
		out["LastModified"] = modified.UTC().Format(time.RFC3339)
	}
	return &spi.Response{Status: 200, Headers: h, Output: out}, nil
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
	if req.S3ValidateSignatures {
		accessKey := postObjectField(fields, "awsaccesskeyid")
		if credential := postObjectField(fields, "x-amz-credential"); credential != "" {
			accessKey, _, _ = strings.Cut(credential, "/")
		}
		secret := "test"
		if accessKey != "test" {
			secret = p.deps.Rand.Derive(accessKey).Hex(40)
		}
		if _, temporary, _ := p.deps.Store.Scope("_mirror", "global").Collection("stsk").Get(ctx, accessKey); temporary {
			if fault := identity.VerifyS3SessionTokenValue(postObjectField(fields, "x-amz-security-token"), p.deps.Rand.Derive(accessKey+"tok").Hex(32)); fault != nil {
				return nil, fault
			}
		}
		if fault := identity.VerifyS3PostPolicy(fields, secret); fault != nil {
			return nil, fault
		}
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
		"x-amz-server-side-encryption":                "ServerSideEncryption",
		"x-amz-server-side-encryption-aws-kms-key-id": "SSEKMSKeyId",
	} {
		if value := fields[form]; value != "" {
			input[member] = value
		}
	}
	if value, ok := fields["x-amz-server-side-encryption-bucket-key-enabled"]; ok {
		input["BucketKeyEnabled"] = truthy(value)
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

func postObjectField(fields map[string]string, name string) string {
	for key, value := range fields {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
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

func verifyPostPolicyCondition(condition any, form map[string]string, bucket string, size int) (b…31184 tokens truncated…:
		return arn, nil
	case "PendingDeletion":
		return "", &spi.Fault{Code: "KMS.KMSInvalidStateException", Message: arn + " is pending deletion.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	default:
		return "", &spi.Fault{Code: "KMS.DisabledException", Message: arn + " is disabled.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
}

func requestSSECustomerKey(req *spi.Request) (string, error) {
	algorithm := requestCondition(req, "SSECustomerAlgorithm", "x-amz-server-side-encryption-customer-algorithm")
	key := requestCondition(req, "SSECustomerKey", "x-amz-server-side-encryption-customer-key")
	keyMD5 := requestCondition(req, "SSECustomerKeyMD5", "x-amz-server-side-encryption-customer-key-MD5")
	if (key != "" || algorithm != "") && requestCondition(req, "ServerSideEncryption", "x-amz-server-side-encryption") != "" {
		return "", &spi.Fault{Code: "InvalidArgument", Message: "Server Side Encryption with Customer provided key is incompatible with the encryption method specified", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-server-side-encryption"}}
	}
	return validateSSECustomerKey(algorithm, key, keyMD5, "x-amz-server-side-encryption-customer-key")
}

func requestCopySourceSSECustomerKey(req *spi.Request) (string, error) {
	return validateSSECustomerKey(
		requestCondition(req, "CopySourceSSECustomerAlgorithm", "x-amz-copy-source-server-side-encryption-customer-algorithm"),
		requestCondition(req, "CopySourceSSECustomerKey", "x-amz-copy-source-server-side-encryption-customer-key"),
		requestCondition(req, "CopySourceSSECustomerKeyMD5", "x-amz-copy-source-server-side-encryption-customer-key-MD5"),
		"x-amz-copy-source-server-side-encryption-customer-key",
	)
}

func validateSSECustomerKey(algorithm, key, keyMD5, argument string) (string, error) {
	invalid := func(code, message string) error {
		return &spi.Fault{Code: code, Message: message, HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": argument}}
	}
	if key == "" && algorithm == "" {
		return "", nil
	}
	if key == "" || algorithm == "" {
		return "", invalid("InvalidArgument", "Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key and a valid encryption algorithm.")
	}
	if algorithm != "AES256" {
		return "", invalid("InvalidEncryptionAlgorithmError", "The Encryption request you specified is not valid. Supported value: AES256.")
	}
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 32 {
		return "", invalid("InvalidArgument", "The secret key was invalid for the specified algorithm.")
	}
	sum := md5.Sum(decoded)
	if base64.StdEncoding.EncodeToString(sum[:]) != keyMD5 {
		return "", invalid("InvalidArgument", "The calculated MD5 hash of the key did not match the hash that was provided.")
	}
	return keyMD5, nil
}

func validateStoredSSECustomerKey(req *spi.Request, meta map[string]any) error {
	stored := str(meta["sseCustomerKeyMD5"])
	provided := requestCondition(req, "SSECustomerKeyMD5", "x-amz-server-side-encryption-customer-key-MD5")
	validated, err := requestSSECustomerKey(req)
	if err != nil {
		return err
	}
	if validated != "" {
		provided = validated
	}
	if stored != provided {
		return &spi.Fault{Code: "InvalidRequest", Message: "The provided encryption parameters did not match the ones used originally.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	return nil
}

func validateCopySourceSSECustomerKey(req *spi.Request, meta map[string]any) error {
	provided := requestCondition(req, "CopySourceSSECustomerKeyMD5", "x-amz-copy-source-server-side-encryption-customer-key-MD5")
	validated, err := requestCopySourceSSECustomerKey(req)
	if err != nil {
		return err
	}
	if validated != "" {
		provided = validated
	}
	if str(meta["sseCustomerKeyMD5"]) != provided {
		return &spi.Fault{Code: "InvalidRequest", Message: "The provided encryption parameters did not match the ones used originally.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	return nil
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
		headers.Set("x-amz-meta-"+key, encodeRFC2047Header(str(value)))
	}
	if redirect := str(meta["websiteRedirectLocation"]); redirect != "" {
		headers.Set("x-amz-website-redirect-location", redirect)
	}
}

func decodeRFC2047Header(value string) string {
	decoded, err := new(mime.WordDecoder).DecodeHeader(value)
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "=?utf-8?b?") && strings.HasSuffix(value, "?=") && len(value) > 13 && (err != nil || decoded == value) {
		return strings.Repeat(string(utf8.RuneError), len(value)-13)
	}
	if strings.HasPrefix(lower, "=?utf-8?q?") && strings.HasSuffix(value, "?=") && decoded == value {
		payload := value[len("=?UTF-8?Q?") : len(value)-2]
		return decodeRFC2047Q(payload)
	}
	if err != nil {
		return value
	}
	return decoded
}

func decodeRFC2047Q(value string) string {
	var decoded strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '_' {
			decoded.WriteByte(' ')
			continue
		}
		if value[i] == '=' && i+2 < len(value) {
			high, highOK := hexDigit(value[i+1])
			low, lowOK := hexDigit(value[i+2])
			if highOK && lowOK {
				decoded.WriteByte(high<<4 | low)
				i += 2
				continue
			}
		}
		decoded.WriteByte(value[i])
	}
	return decoded.String()
}

func hexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	default:
		return 0, false
	}
}

func encodeRFC2047Header(value string) string {
	const safe = "!\"#$%&'()*+,-./0123456789:;<>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^`abcdefghijklmnopqrstuvwxyz{|}~\t"
	const unencoded = safe + " _=?"
	if strings.IndexFunc(value, func(r rune) bool {
		return r > unicode.MaxASCII || !strings.ContainsRune(unencoded, r)
	}) < 0 {
		return value
	}
	for _, r := range value {
		if r == utf8.RuneError || unicode.In(r, unicode.Cc, unicode.Cf, unicode.Co, unicode.Cs) {
			return "=?UTF-8?B?" + base64.StdEncoding.EncodeToString([]byte(value)) + "?="
		}
	}
	var encoded strings.Builder
	encoded.WriteString("=?UTF-8?Q?")
	const hex = "0123456789ABCDEF"
	for _, b := range []byte(value) {
		switch {
		case b == ' ':
			encoded.WriteByte('_')
		case strings.IndexByte(safe, b) >= 0:
			encoded.WriteByte(b)
		default:
			encoded.WriteByte('=')
			encoded.WriteByte(hex[b>>4])
			encoded.WriteByte(hex[b&15])
		}
	}
	encoded.WriteString("?=")
	return encoded.String()
}

func setObjectEncryptionHeaders(headers http.Header, meta map[string]any) {
	if keyMD5 := str(meta["sseCustomerKeyMD5"]); keyMD5 != "" {
		headers.Set("x-amz-server-side-encryption-customer-algorithm", "AES256")
		headers.Set("x-amz-server-side-encryption-customer-key-MD5", keyMD5)
		return
	}
	encryption := str(meta["serverSideEncryption"])
	if encryption == "" {
		return
	}
	headers.Set("x-amz-server-side-encryption", encryption)
	if encryption != "aws:kms" {
		return
	}
	if keyID := str(meta["ssekmsKeyId"]); keyID != "" {
		headers.Set("x-amz-server-side-encryption-aws-kms-key-id", keyID)
	}
	if truthy(meta["bucketKeyEnabled"]) {
		headers.Set("x-amz-server-side-encryption-bucket-key-enabled", "true")
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

func (p *Pack) validateGovernanceBypass(ctx context.Context, req *spi.Request, bucket string) error {
	if set, _ := governanceBypass(req); set && !p.bucketObjectLockEnabled(ctx, req, bucket) {
		return &spi.Fault{Code: "InvalidArgument", Message: "x-amz-bypass-governance-retention is only applicable to Object Lock enabled buckets.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ArgumentName": "x-amz-bypass-governance-retention"}}
	}
	return nil
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
	notificationMeta := cloneMap(meta)
	notificationMeta["restoreExpiry"] = expires.UTC().Format("2006-01-02T15:04:05.000Z")
	p.notify(ctx, req, b, key, "ObjectRestore:Post", notificationMeta)
	p.notify(ctx, req, b, key, "ObjectRestore:Completed", notificationMeta)
	status := http.StatusAccepted
	if restored {
		status = http.StatusOK
	}
	return &spi.Response{Status: status, Output: map[string]any{}}, nil
}

func (p *Pack) namedCfg(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	b := str(req.Input["Bucket"])
	spec := namedConfigurationSpecFor(req.Operation)
	var err error
	if spec.kind == "intelligent" {
		err = p.requireBucketOwner(ctx, req, b, "")
	} else {
		err = p.requireBucket(ctx, req, b)
	}
	if err != nil {
		return nil, err
	}
	id := str(req.Input["Id"])
	if id == "" {
		id = str(req.Input["id"])
	}
	collection := p.col(req, "namedcfg")
	prefix := b + "/" + spec.kind + "/"
	if strings.HasPrefix(req.Operation, "List") {
		kvs, _, _ := collection.List(ctx, prefix, "", 0)
		items := make([]any, 0, len(kvs))
		for _, kv := range kvs {
			var doc map[string]any
			_ = json.Unmarshal(kv.Value, &doc)
			items = append(items, doc)
		}
		sort.Slice(items, func(i, j int) bool { return str(asMap(items[i])["Id"]) < str(asMap(items[j])["Id"]) })
		output := map[string]any{"IsTruncated": false, spec.list: items}
		if spec.kind == "metrics" {
			token := str(req.Input["ContinuationToken"])
			if token == "" {
				token = str(req.Input["continuation-token"])
			}
			start := 0
			if token != "" {
				decoded, err := base64.URLEncoding.DecodeString(token)
				if err != nil {
					return nil, &spi.Fault{Code: "InvalidToken", Message: "The continuation token provided is incorrect", HTTPStatus: http.StatusBadRequest, Fault: "client"}
				}
				marker := string(decoded)
				for start < len(items) && str(asMap(items[start])["Id"]) < marker {
					start++
				}
				output["ContinuationToken"] = token
			}
			end := min(start+100, len(items))
			if end < len(items) {
				output["IsTruncated"] = true
				output["NextContinuationToken"] = base64.URLEncoding.EncodeToString([]byte(str(asMap(items[end])["Id"])))
			}
			output[spec.list] = items[start:end]
		}
		return &spi.Response{Status: http.StatusOK, Output: output}, nil
	}
	if id == "" {
		return nil, &spi.Fault{Code: "InvalidArgument", Message: "The configuration ID is required", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	ck := prefix + id
	if strings.HasPrefix(req.Operation, "Put") {
		configuration, ok := req.Input[spec.configuration].(map[string]any)
		if !ok {
			return nil, malformedXML()
		}
		if err := validateNamedConfiguration(spec.kind, id, configuration); err != nil {
			return nil, err
		}
		if spec.kind == "metrics" {
			if _, exists, _ := collection.Get(ctx, ck); !exists {
				kvs, _, _ := collection.List(ctx, prefix, "", 1001)
				if len(kvs) >= 1000 {
					return nil, &spi.Fault{Code: "TooManyConfigurations", Message: "Too many metrics configurations", HTTPStatus: http.StatusBadRequest, Fault: "client"}
				}
			}
		}
		raw, _ := json.Marshal(configuration)
		_ = collection.Put(ctx, ck, raw)
		return &spi.Response{Status: 200, Output: map[string]any{}}, nil
	}
	if strings.HasPrefix(req.Operation, "Delete") {
		if _, exists, _ := collection.Get(ctx, ck); !exists {
			return nil, &spi.Fault{Code: "NoSuchConfiguration", Message: "The specified configuration does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client"}
		}
		_ = collection.Delete(ctx, ck)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	}
	raw, ok, _ := collection.Get(ctx, ck)
	if !ok {
		return nil, &spi.Fault{Code: "NoSuchConfiguration", Message: "The specified configuration does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client"}
	}
	var doc map[string]any
	_ = json.Unmarshal(raw, &doc)
	return &spi.Response{Status: 200, Output: map[string]any{spec.configuration: doc}}, nil
}

type namedConfigurationSpec struct{ kind, configuration, list string }

func namedConfigurationSpecFor(operation string) namedConfigurationSpec {
	switch {
	case strings.Contains(operation, "Inventory"):
		return namedConfigurationSpec{"inventory", "InventoryConfiguration", "InventoryConfigurationList"}
	case strings.Contains(operation, "Intelligent"):
		return namedConfigurationSpec{"intelligent", "IntelligentTieringConfiguration", "IntelligentTieringConfigurationList"}
	case strings.Contains(operation, "Metrics"):
		return namedConfigurationSpec{"metrics", "MetricsConfiguration", "MetricsConfigurationList"}
	default:
		return namedConfigurationSpec{"analytics", "AnalyticsConfiguration", "AnalyticsConfigurationList"}
	}
}

func validateNamedConfiguration(kind, id string, configuration map[string]any) error {
	if kind == "inventory" {
		return validateInventoryConfiguration(id, configuration)
	}
	if (kind == "analytics" || kind == "intelligent") && str(configuration["Id"]) != id {
		return malformedXML()
	}
	return nil
}

func validateInventoryConfiguration(id string, configuration map[string]any) error {
	if !hasOnlyFields(configuration,
		[]string{"Destination", "Id", "IncludedObjectVersions", "IsEnabled", "Schedule"},
		[]string{"Filter", "OptionalFields"}) {
		return malformedXML()
	}
	destination := asMap(asMap(configuration["Destination"])["S3BucketDestination"])
	if !hasOnlyFields(destination, []string{"Bucket", "Format"}, []string{"AccountId", "Encryption", "Prefix"}) {
		return malformedXML()
	}
	if format := str(destination["Format"]); format != "CSV" && format != "ORC" && format != "Parquet" {
		return malformedXML()
	}
	frequency := str(asMap(configuration["Schedule"])["Frequency"])
	if frequency != "Daily" && frequency != "Weekly" {
		return malformedXML()
	}
	versions := str(configuration["IncludedObjectVersions"])
	if versions != "All" && versions != "Current" {
		return malformedXML()
	}
	allowed := map[string]bool{
		"Size": true, "LastModifiedDate": true, "StorageClass": true, "ETag": true,
		"IsMultipartUploaded": true, "ReplicationStatus": true, "EncryptionStatus": true,
		"ObjectLockRetainUntilDate": true, "ObjectLockMode": true, "ObjectLockLegalHoldStatus": true,
		"IntelligentTieringAccessTier": true, "BucketKeyStatus": true, "ChecksumAlgorithm": true,
	}
	for _, value := range asSlice(configuration["OptionalFields"]) {
		if !allowed[str(value)] {
			return malformedXML()
		}
	}
	if str(configuration["Id"]) != id {
		return &spi.Fault{Code: "IdMismatch", Message: "Document ID does not match the specified configuration ID.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	parts := strings.SplitN(str(destination["Bucket"]), ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "s3" || parts[5] == "" {
		return &spi.Fault{Code: "InvalidS3DestinationBucket", Message: "Invalid bucket ARN.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	return nil
}

func hasOnlyFields(value map[string]any, required, optional []string) bool {
	allowed := make(map[string]bool, len(required)+len(optional))
	for _, field := range required {
		allowed[field] = true
	}
	for _, field := range optional {
		allowed[field] = true
	}
	for _, field := range required {
		if _, exists := value[field]; !exists {
			return false
		}
	}
	for field := range value {
		if !allowed[field] {
			return false
		}
	}
	return true
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
	_, ok, err := p.col(req, "buckets").Get(ctx, b)
	if err != nil {
		return err
	}
	if !ok {
		raw, exists, err := p.deps.Store.Scope("_mirror", "global").Collection("s3buckets").Get(ctx, b)
		if err != nil {
			return err
		}
		if exists {
			var location struct {
				Region string `json:"region"`
			}
			if err := json.Unmarshal(raw, &location); err != nil {
				return err
			}
			if location.Region != "" {
				req.Identity.Region = location.Region
				_, ok, err = p.col(req, "buckets").Get(ctx, b)
				if err != nil {
					return err
				}
			}
		}
	}
	if !ok {
		return &spi.Fault{Code: "NoSuchBucket", Message: "The specified bucket does not exist", HTTPStatus: 404, Fault: "client"}
	}
	if expected != "" && expected != req.Identity.Account {
		return &spi.Fault{Code: "AccessDenied", Message: "Access Denied", HTTPStatus: http.StatusForbidden, Fault: "client"}
	}
	return nil
}

func (p *Pack) notify(ctx context.Context, req *spi.Request, bucket, key, event string, metadata ...map[string]any) {
	meta := map[string]any{}
	if len(metadata) > 0 {
		meta = metadata[0]
	} else {
		meta, _ = p.objectMetadata(ctx, req, bucket, key, str(req.Input["VersionId"]))
	}
	payload := p.notificationPayload(req, bucket, key, event, "", meta)
	_ = p.deps.Bus.Publish(ctx, "s3:"+bucket, payload)
	raw, ok, _ := p.col(req, "notify").Get(ctx, bucket)
	if !ok {
		return
	}
	var cfg map[string]any
	_ = json.Unmarshal(raw, &cfg)
	for _, dest := range append(asSlice(cfg["QueueConfigurations"]), asSlice(cfg["TopicConfigurations"])...) {
		m := asMap(dest)
		if !notificationMatches(m, key, event) {
			continue
		}
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
		payload = p.notificationPayload(req, bucket, key, event, str(m["Id"]), meta)
		name := arn
		if i := strings.LastIndex(arn, ":"); i >= 0 {
			name = arn[i+1:]
		}
		if str(m["QueueArn"]) != "" || str(m["Queue"]) != "" || strings.Contains(arn, ":sqs:") {
			input := map[string]any{"QueueName": name, "MessageBody": string(payload)}
			if req.HTTP != nil && req.HTTP.Header.Get("X-Amzn-Trace-Id") != "" {
				input["MessageSystemAttributes"] = map[string]any{"AWSTraceHeader": map[string]any{"DataType": "String", "StringValue": req.HTTP.Header.Get("X-Amzn-Trace-Id")}}
			}
			_, _ = sqs.New(p.deps).Invoke(ctx, &spi.Request{
				Identity: notificationTargetIdentity(req.Identity, arn), Operation: "SendMessage",
				Input: input,
			})
			continue
		}
		_, _ = sns.New(p.deps).Invoke(ctx, &spi.Request{
			Identity: notificationTargetIdentity(req.Identity, arn), Operation: "Publish",
			Input: map[string]any{"TopicArn": arn, "Message": string(payload), "Subject": "Amazon S3 Notification"},
		})
	}
	for _, dest := range asSlice(cfg["LambdaFunctionConfigurations"]) {
		configuration := asMap(dest)
		if !notificationMatches(configuration, key, event) {
			continue
		}
		arn := str(configuration["LambdaFunctionArn"])
		payload = p.notificationPayload(req, bucket, key, event, str(configuration["Id"]), meta)
		_, name, found := strings.Cut(arn, ":function:")
		if !found {
			continue
		}
		name, _, _ = strings.Cut(name, ":")
		_, _ = lambda.New(p.deps).Invoke(ctx, &spi.Request{
			Identity: notificationTargetIdentity(req.Identity, arn), Operation: "Invoke", Body: io.NopCloser(bytes.NewReader(payload)),
			Input: map[string]any{"FunctionName": name, "InvocationType": "Event"},
		})
	}
	if _, enabled := cfg["EventBridgeConfiguration"]; enabled {
		entry := p.eventBridgeEntry(req, bucket, key, event, meta)
		envelope, _ := json.Marshal(map[string]any{"identity": req.Identity, "entry": entry})
		_ = p.deps.Bus.Publish(ctx, "events:s3", envelope)
	}
}

func notificationTargetIdentity(identity spi.Identity, arn string) spi.Identity {
	parts := strings.Split(arn, ":")
	if len(parts) >= 6 {
		identity.Region, identity.Account = parts[3], parts[4]
	}
	return identity
}

func (p *Pack) notificationPayload(req *spi.Request, bucket, key, event, configurationID string, meta map[string]any) []byte {
	object := map[string]any{"key": notificationKey(key), "sequencer": "0055AED6DCD90281E5"}
	if version := str(meta["versionId"]); version != "" && version != "null" {
		object["versionId"] = version
	}
	if strings.Contains(event, "ObjectCreated") || strings.Contains(event, "ObjectRestore") {
		object["eTag"] = strings.Trim(str(meta["etag"]), `"`)
		object["size"] = asInt(meta["size"])
	}
	eventVersion := "2.1"
	if strings.Contains(event, "ObjectTagging") || strings.Contains(event, "ObjectAcl") {
		eventVersion = "2.3"
		object["eTag"] = strings.Trim(str(meta["etag"]), `"`)
		delete(object, "sequencer")
	}
	eventTime := p.deps.Clock.Now().UTC()
	if strings.HasSuffix(event, "ObjectRestore:Completed") {
		eventTime = eventTime.Add(500 * time.Millisecond)
	}
	record := map[string]any{
		"eventVersion": eventVersion, "eventSource": "aws:s3", "awsRegion": req.Identity.Region,
		"eventTime": eventTime.Format("2006-01-02T15:04:05.000Z"), "eventName": event,
		"userIdentity":      map[string]any{"principalId": "AIDAJDPLRKLG7UEXAMPLE"},
		"requestParameters": map[string]any{"sourceIPAddress": "127.0.0.1"},
		"responseElements":  map[string]any{"x-amz-request-id": "mirror", "x-amz-id-2": "mirror"},
		"s3": map[string]any{
			"s3SchemaVersion": "1.0", "configurationId": configurationID,
			"bucket": map[string]any{"name": bucket, "ownerIdentity": map[string]any{"principalId": req.Identity.Account}, "arn": "arn:aws:s3:::" + bucket},
			"object": object,
		},
	}
	if strings.HasSuffix(event, "ObjectRestore:Completed") {
		record["glacierEventData"] = map[string]any{"restoreEventData": map[string]any{
			"lifecycleRestorationExpiryTime": str(meta["restoreExpiry"]), "lifecycleRestoreStorageClass": str(meta["storageClass"]),
		}}
	}
	payload, _ := json.Marshal(map[string]any{"Records": []any{record}})
	return payload
}

func (p *Pack) eventBridgeEntry(req *spi.Request, bucket, key, event string, meta map[string]any) map[string]any {
	object := map[string]any{
		"key": notificationKey(key), "size": asInt(meta["size"]),
		"etag": strings.Trim(str(meta["etag"]), `"`), "sequencer": "0062E99A88DC407460",
	}
	if version := str(meta["versionId"]); version != "" && version != "null" {
		object["version-id"] = version
	}
	detail := map[string]any{
		"version": "0", "bucket": map[string]any{"name": bucket}, "object": object,
		"request-id": "mirror", "requester": req.Identity.Account, "source-ip-address": "127.0.0.1",
	}
	detailType := ""
	switch {
	case strings.Contains(event, "ObjectCreated"):
		detailType = "Object Created"
		action := event[strings.LastIndex(event, ":")+1:]
		if action == "Put" || action == "Post" || action == "Copy" {
			detail["reason"] = action + "Object"
		} else {
			detail["reason"] = "s3:" + event
		}
	case strings.Contains(event, "ObjectRemoved"):
		detailType, detail["reason"] = "Object Deleted", "DeleteObject"
		delete(object, "size")
		if strings.Contains(event, "DeleteMarkerCreated") {
			detail["deletion-type"] = "Delete Marker Created"
			object["etag"] = "d41d8cd98f00b204e9800998ecf8427e"
		} else {
			detail["deletion-type"] = "Permanently Deleted"
			delete(object, "etag")
		}
	case strings.Contains(event, "ObjectTagging"):
		if strings.HasSuffix(event, ":Put") {
			detailType = "Object Tags Added"
		} else {
			detailType = "Object Tags Deleted"
		}
	case strings.Contains(event, "ObjectAcl"):
		detailType = "Object ACL Updated"
		delete(object, "size")
		delete(object, "sequencer")
	case strings.Contains(event, "ObjectRestore"):
		detailType = "Object Restore Initiated"
		if strings.HasSuffix(event, ":Completed") {
			detailType = "Object Restore Completed"
			detail["restore-expiry-time"] = str(meta["restoreExpiry"])
			delete(detail, "source-ip-address")
		}
		detail["source-storage-class"] = str(meta["storageClass"])
		delete(object, "sequencer")
	}
	detailJSON, _ := json.Marshal(detail)
	eventTime := p.deps.Clock.Now().UTC()
	if strings.HasSuffix(event, "ObjectRestore:Completed") {
		eventTime = eventTime.Add(time.Second)
	}
	return map[string]any{
		"Source": "aws.s3", "Resources": []any{"arn:aws:s3:::" + bucket},
		"Time": eventTime.Format(time.RFC3339Nano), "DetailType": detailType, "Detail": string(detailJSON),
	}
}

func notificationKey(key string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(key), "+", "%20"), "%2F", "/")
}

func notificationMatches(configuration map[string]any, key, event string) bool {
	event = "s3:" + event
	wildcard := event[:strings.LastIndex(event, ":")+1] + "*"
	matched := false
	for _, value := range asSlice(configuration["Events"]) {
		if configured := str(value); configured == event || configured == wildcard {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}
	for _, value := range asSlice(asMap(asMap(configuration["Filter"])["Key"])["FilterRules"]) {
		rule := asMap(value)
		switch strings.ToLower(str(rule["Name"])) {
		case "prefix":
			if !strings.HasPrefix(key, str(rule["Value"])) {
				return false
			}
		case "suffix":
			if !strings.HasSuffix(key, str(rule["Value"])) {
				return false
			}
		}
	}
	return true
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
	if err := validateCopySourceSSECustomerKey(req, meta); err != nil {
		return nil, err
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

func (p *Pack) restoreCurrentVersion(ctx context.Context, req *spi.Request, bucket, key, version string, order []string, deletedVersionNext map[string]any) error {
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
	meta["deletedVersionNext"] = deletedVersionNext
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

func objectReadNotFound(key, version string) *spi.Fault {
	if version != "" {
		return &spi.Fault{Code: "NoSuchVersion", Message: "The specified version does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client", Fields: map[string]any{"Key": key, "VersionId": version}}
	}
	return &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client", Fields: map[string]any{"Key": key}}
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

var continuationEncoding = base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._")

func checksumByAlgorithm(algorithm string) (struct{ algorithm, input, header string }, bool) {
	for _, checksum := range checksums {
		if checksum.algorithm == algorithm {
			return checksum, true
		}
	}
	return struct{ algorithm, input, header string }{}, false
}

func validateMultipartChecksumContract(req *spi.Request, algorithm, checksumType string) error {
	if algorithm == "" {
		if checksumType != "" {
			return &spi.Fault{Code: "InvalidRequest", Message: "The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
		}
		return nil
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
	if value := requestCondition(req, "ContentMD5", "Content-MD5"); value != "" {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil || len(decoded) != md5.Size {
			return &spi.Fault{Code: "InvalidDigest", Message: "The Content-MD5 you specified was invalid.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"Content_MD5": value}}
		}
		calculated := checksumValue("ChecksumMD5", body)
		if value != calculated {
			return &spi.Fault{Code: "BadDigest", Message: "The Content-MD5 you specified did not match what we received.", HTTPStatus: http.StatusBadRequest, Fault: "client", Fields: map[string]any{"ExpectedDigest": value, "CalculatedDigest": calculated}}
		}
	}
	for _, checksum := range checksums {
		if value := requestCondition(req, checksum.input, checksum.header); value != "" {
			if checksum.algorithm != selected.algorithm {
				return &spi.Fault{Code: "InvalidRequest", Message: fmt.Sprintf("Checksum Type mismatch occurred, expected checksum Type: %s, actual checksum Type: %s", strings.ToLower(selected.algorithm), strings.ToLower(checksum.algorithm)), HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
			calculated := checksumValue(checksum.input, body)
			decoded, err := base64.StdEncoding.DecodeString(value)
			expected, _ := base64.StdEncoding.DecodeString(calculated)
			if err != nil || len(decoded) != len(expected) {
				return &spi.Fault{Code: "InvalidRequest", Message: "Value for " + checksum.header + " header is invalid.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
			}
			if value != calculated {
				return &spi.Fault{Code: "BadDigest", Message: fmt.Sprintf("The %s you specified did not match the calculated checksum.", checksum.algorithm), HTTPStatus: http.StatusBadRequest, Fault: "client"}
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

func setListChecksumMetadata(row, meta map[string]any) {
	stored := asMap(meta["checksums"])
	var algorithms []any
	for _, checksum := range checksums {
		if str(stored[checksum.header]) != "" {
			algorithms = append(algorithms, checksum.algorithm)
		}
	}
	if len(algorithms) > 0 {
		row["ChecksumAlgorithm"] = algorithms
		row["ChecksumType"] = str(meta["checksumType"])
	}
}

func etagMatches(condition, etag string) bool {
	condition = strings.TrimSpace(condition)
	return condition == "*" || strings.Trim(condition, `"`) == strings.Trim(etag, `"`)
}

func preconditionFailed(condition string) error {
	return &spi.Fault{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold", HTTPStatus: 412, Fault: "client", Fields: map[string]any{"Condition": condition}}
}

func checkReadPreconditions(req *spi.Request, etag, modified string, now time.Time) (bool, error) {
	if match := requestCondition(req, "IfMatch", "If-Match"); match != "" {
		if !etagMatches(match, etag) {
			return false, preconditionFailed("If-Match")
		}
	} else if value := requestCondition(req, "IfUnmodifiedSince", "If-Unmodified-Since"); value != "" {
		if condition, err := http.ParseTime(value); err == nil && sourceModifiedAfter(modified, condition) {
			return false, preconditionFailed("If-Unmodified-Since")
		}
	}
	if noneMatch := requestCondition(req, "IfNoneMatch", "If-None-Match"); noneMatch != "" {
		return etagMatches(noneMatch, etag), nil
	}
	if value := requestCondition(req, "IfModifiedSince", "If-Modified-Since"); value != "" {
		if condition, err := http.ParseTime(value); err == nil && !sourceModifiedAfter(modified, condition) && condition.Before(now) {
			return true, nil
		}
	}
	return false, nil
}

func (p *Pack) checkWritePreconditions(ctx context.Context, req *spi.Request, bucket, key string) error {
	match := requestCondition(req, "IfMatch", "If-Match")
	noneMatch := requestCondition(req, "IfNoneMatch", "If-None-Match")
	if noneMatch != "" && match != "" {
		return &spi.Fault{Code: "NotImplemented", Message: "A header you provided implies functionality that is not implemented", HTTPStatus: http.StatusNotImplemented, Fault: "server", Fields: map[string]any{"Header": "If-Match,If-None-Match", "additionalMessage": "Multiple conditional request headers present in the request"}}
	} else if noneMatch != "*" && noneMatch != "" {
		return &spi.Fault{Code: "NotImplemented", Message: "A header you provided implies functionality that is not implemented", HTTPStatus: http.StatusNotImplemented, Fault: "server", Fields: map[string]any{"Header": "If-None-Match", "additionalMessage": "We don't accept the provided value of If-None-Match header for this API"}}
	} else if match == "*" && noneMatch == "" {
		return &spi.Fault{Code: "NotImplemented", Message: "A header you provided implies functionality that is not implemented", HTTPStatus: http.StatusNotImplemented, Fault: "server", Fields: map[string]any{"Header": "If-None-Match", "additionalMessage": "We don't accept the provided value of If-None-Match header for this API"}}
	}
	if match == "" && noneMatch == "" {
		return nil
	}
	raw, exists, _ := p.col(req, "objects").Get(ctx, bucket+"/"+key)
	var meta map[string]any
	_ = json.Unmarshal(raw, &meta)
	exists = exists && !truthy(meta["deleteMarker"])
	etag := str(meta["etag"])
	if !exists && match != "" {
		return &spi.Fault{Code: "NoSuchKey", Message: "The specified key does not exist.", HTTPStatus: http.StatusNotFound, Fault: "client", Fields: map[string]any{"Key": key}}
	}
	if match != "" && strings.Trim(match, "\"") != strings.Trim(etag, "\"") && exists {
		return &spi.Fault{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold", HTTPStatus: http.StatusPreconditionFailed, Fault: "client", Fields: map[string]any{"Condition": "If-Match"}}
	}
	if noneMatch != "" && exists && etagMatches(noneMatch, etag) {
		return &spi.Fault{Code: "PreconditionFailed", Message: "At least one of the pre-conditions you specified did not hold", HTTPStatus: http.StatusPreconditionFailed, Fault: "client", Fields: map[string]any{"Condition": "If-None-Match"}}
	}
	return nil
}

func checkCopySourcePreconditions(req *spi.Request, etag, modified string, now time.Time) error {
	match := requestCondition(req, "CopySourceIfMatch", "x-amz-copy-source-if-match")
	if match != "" {
		if !etagMatches(match, etag) {
			return preconditionFailed("x-amz-copy-source-If-Match")
		}
		if value := requestCondition(req, "CopySourceIfModifiedSince", "x-amz-copy-source-if-modified-since"); value != "" {
			condition, conditionErr := http.ParseTime(value)
			modifiedAt, modifiedErr := http.ParseTime(modified)
			if conditionErr != nil || modifiedErr != nil || condition.After(modifiedAt) {
				return preconditionFailed("x-amz-copy-source-If-Modified-Since")
			}
		}
		if requestCondition(req, "CopySourceIfUnmodifiedSince", "x-amz-copy-source-if-unmodified-since") != "" {
			return nil
		}
	}
	if value := requestCondition(req, "CopySourceIfUnmodifiedSince", "x-amz-copy-source-if-unmodified-since"); value != "" {
		if condition, err := http.ParseTime(value); err != nil || sourceModifiedAfter(modified, condition) {
			return preconditionFailed("x-amz-copy-source-If-Unmodified-Since")
		}
	}
	noneMatch := requestCondition(req, "CopySourceIfNoneMatch", "x-amz-copy-source-if-none-match")
	if noneMatch != "" {
		if etagMatches(noneMatch, etag) {
			return preconditionFailed("x-amz-copy-source-If-None-Match")
		}
	}
	if value := requestCondition(req, "CopySourceIfModifiedSince", "x-amz-copy-source-if-modified-since"); value != "" {
		if condition, err := http.ParseTime(value); err != nil || !sourceModifiedAfter(modified, condition) && condition.Before(now) {
			return preconditionFailed("x-amz-copy-source-If-Modified-Since")
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
	return p.versioningStatus(ctx, req, b) == "Enabled"
}

func (p *Pack) versioningStatus(ctx context.Context, req *spi.Request, b string) string {
	raw, ok, _ := p.col(req, "versioning").Get(ctx, b)
	if !ok {
		return ""
	}
	return string(raw)
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
		return 0, 0, true, &spi.Fault{Code: "InvalidRange", Message: "The requested range is not satisfiable", HTTPStatus: http.StatusRequestedRangeNotSatisfiable, Fault: "client", Fields: map[string]any{"ActualObjectSize": strconv.FormatInt(size, 10), "RangeRequested": value}, Headers: h}
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
