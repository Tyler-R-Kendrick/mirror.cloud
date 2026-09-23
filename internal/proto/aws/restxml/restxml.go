// Package restxml implements the S3 restXml codec.
package restxml

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/xmlenc"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for restXml.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoRESTXML }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	if a := r.URL.Query().Get("Action"); a != "" {
		if op := svc.OperationByName(a); op != nil {
			return op, nil
		}
		return &model.Operation{Name: a, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}, nil
	}
	if svc.ID == "azure.blobs" {
		return azureOp(svc, r), nil
	}
	if svc.ID == "azure.queue" {
		return azureQueueOp(svc, r), nil
	}
	if svc.ID == "aws.route53" {
		return route53Op(svc, r), nil
	}
	if svc.ID == "aws.cloudfront" {
		return cloudfrontOp(svc, r), nil
	}
	name := r.Header.Get("X-Mirror-Operation")
	if name == "" {
		name = RouteName(r)
	}
	if name == "" && svc.ID == "aws.s3" && r.Method == http.MethodOptions && svc.OperationByName("GetObject") != nil {
		name = "GetObject"
	}
	if name != "" {
		if op := svc.OperationByName(name); op != nil {
			return op, nil
		}
		return &model.Operation{Name: name}, nil
	}
	if len(svc.Operations) > 0 {
		return &svc.Operations[0], nil
	}
	return nil, spi.NotImplemented(svc.ID, r.Method+" "+r.URL.Path, "emulate")
}

func hasQuery(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

func bucketKey(r *http.Request) (bucket, key string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if bucket, ok := websiteBucketHost(host); ok {
		return bucket, path
	}
	if strings.Contains(host, ".s3.") {
		return strings.Split(host, ".s3.")[0], path
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) > 0 {
		bucket = parts[0]
	}
	if len(parts) > 1 {
		key = parts[1]
	}
	return bucket, key
}

func websiteBucketHost(host string) (string, bool) {
	lower := strings.ToLower(host)
	for _, marker := range []string{".s3-website.", ".s3-website-"} {
		if index := strings.Index(lower, marker); index > 0 {
			return host[:index], true
		}
	}
	return "", false
}

// RouteName maps an S3 REST request to the Smithy operation name.
// Query flags are matched by key presence (`?tagging`, `?delete`), not Get() != "".
func RouteName(r *http.Request) string {
	if _, ok := websiteBucketHost(strings.Split(r.Host, ":")[0]); ok {
		return "GetObject"
	}
	bucket, key := bucketKey(r)
	m := r.Method
	switch {
	case hasQuery(r, "tagging"):
		if key == "" {
			return putGetDel(m, "PutBucketTagging", "GetBucketTagging", "DeleteBucketTagging")
		}
		return putGetDel(m, "PutObjectTagging", "GetObjectTagging", "DeleteObjectTagging")
	case hasQuery(r, "notification"):
		return putOrGet(m, "PutBucketNotificationConfiguration", "GetBucketNotificationConfiguration")
	case hasQuery(r, "versioning"):
		return putOrGet(m, "PutBucketVersioning", "GetBucketVersioning")
	case hasQuery(r, "acl"):
		if key == "" {
			return putOrGet(m, "PutBucketAcl", "GetBucketAcl")
		}
		return putOrGet(m, "PutObjectAcl", "GetObjectAcl")
	case hasQuery(r, "policy"):
		if m == http.MethodDelete {
			return "DeleteBucketPolicy"
		}
		return putOrGet(m, "PutBucketPolicy", "GetBucketPolicy")
	case hasQuery(r, "cors"):
		if m == http.MethodDelete {
			return "DeleteBucketCors"
		}
		return putOrGet(m, "PutBucketCors", "GetBucketCors")
	case hasQuery(r, "website"):
		if m == http.MethodDelete {
			return "DeleteBucketWebsite"
		}
		return putOrGet(m, "PutBucketWebsite", "GetBucketWebsite")
	case hasQuery(r, "logging"):
		return putOrGet(m, "PutBucketLogging", "GetBucketLogging")
	case hasQuery(r, "lifecycle"):
		if m == http.MethodDelete {
			return "DeleteBucketLifecycle"
		}
		return putOrGet(m, "PutBucketLifecycleConfiguration", "GetBucketLifecycleConfiguration")
	case hasQuery(r, "replication"):
		return putGetDel(m, "PutBucketReplication", "GetBucketReplication", "DeleteBucketReplication")
	case hasQuery(r, "session"):
		return "CreateSession"
	case hasQuery(r, "select"):
		return "SelectObjectContent"
	case hasQuery(r, "torrent"):
		return "GetObjectTorrent"
	case hasQuery(r, "abac"):
		return putOrGet(m, "PutBucketAbac", "GetBucketAbac")
	case hasQuery(r, "metadataTable"):
		return putGetDel(m, "CreateBucketMetadataTableConfiguration", "GetBucketMetadataTableConfiguration", "DeleteBucketMetadataTableConfiguration")
	case hasQuery(r, "metadataConfiguration") || hasQuery(r, "metadata"):
		if m == http.MethodPost {
			if hasQuery(r, "inventory") {
				return "UpdateBucketMetadataInventoryTableConfiguration"
			}
			if hasQuery(r, "journal") {
				return "UpdateBucketMetadataJournalTableConfiguration"
			}
			return "UpdateBucketMetadataAnnotationTableConfiguration"
		}
		return putGetDel(m, "CreateBucketMetadataConfiguration", "GetBucketMetadataConfiguration", "DeleteBucketMetadataConfiguration")
	case hasQuery(r, "annotation"):
		if key == "" {
			return "ListObjectAnnotations"
		}
		return putGetDel(m, "PutObjectAnnotation", "GetObjectAnnotation", "DeleteObjectAnnotation")
	case hasQuery(r, "rename") || r.Header.Get("x-amz-rename-source") != "":
		return "RenameObject"
	case hasQuery(r, "encryption"):
		if m == http.MethodDelete {
			return "DeleteBucketEncryption"
		}
		return putOrGet(m, "PutBucketEncryption", "GetBucketEncryption")
	case hasQuery(r, "object-lock"):
		return putOrGet(m, "PutObjectLockConfiguration", "GetObjectLockConfiguration")
	case hasQuery(r, "requestPayment"):
		return putOrGet(m, "PutBucketRequestPayment", "GetBucketRequestPayment")
	case hasQuery(r, "accelerate"):
		return putOrGet(m, "PutBucketAccelerateConfiguration", "GetBucketAccelerateConfiguration")
	case hasQuery(r, "publicAccessBlock"):
		return putGetDel(m, "PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock")
	case hasQuery(r, "ownershipControls"):
		return putGetDel(m, "PutBucketOwnershipControls", "GetBucketOwnershipControls", "DeleteBucketOwnershipControls")
	case hasQuery(r, "policyStatus") && m == http.MethodGet:
		return "GetBucketPolicyStatus"
	case hasQuery(r, "attributes") && m == http.MethodGet:
		return "GetObjectAttributes"
	case hasQuery(r, "legal-hold"):
		return putOrGet(m, "PutObjectLegalHold", "GetObjectLegalHold")
	case hasQuery(r, "retention"):
		return putOrGet(m, "PutObjectRetention", "GetObjectRetention")
	case hasQuery(r, "restore") && m == http.MethodPost:
		return "RestoreObject"
	case hasQuery(r, "analytics"):
		if r.URL.Query().Get("id") != "" {
			return putGetDel(m, "PutBucketAnalyticsConfiguration", "GetBucketAnalyticsConfiguration", "DeleteBucketAnalyticsConfiguration")
		}
		return "ListBucketAnalyticsConfigurations"
	case hasQuery(r, "inventory"):
		if r.URL.Query().Get("id") != "" {
			return putGetDel(m, "PutBucketInventoryConfiguration", "GetBucketInventoryConfiguration", "DeleteBucketInventoryConfiguration")
		}
		return "ListBucketInventoryConfigurations"
	case hasQuery(r, "metrics"):
		if r.URL.Query().Get("id") != "" {
			return putGetDel(m, "PutBucketMetricsConfiguration", "GetBucketMetricsConfiguration", "DeleteBucketMetricsConfiguration")
		}
		return "ListBucketMetricsConfigurations"
	case hasQuery(r, "intelligent-tiering"):
		if r.URL.Query().Get("id") != "" {
			return putGetDel(m, "PutBucketIntelligentTieringConfiguration", "GetBucketIntelligentTieringConfiguration", "DeleteBucketIntelligentTieringConfiguration")
		}
		return "ListBucketIntelligentTieringConfigurations"
	case hasQuery(r, "location") && m == http.MethodGet:
		return "GetBucketLocation"
	case hasQuery(r, "versions") && m == http.MethodGet:
		return "ListObjectVersions"
	case hasQuery(r, "delete") && m == http.MethodPost:
		return "DeleteObjects"
	case hasQuery(r, "uploads") && m == http.MethodPost:
		return "CreateMultipartUpload"
	case hasQuery(r, "uploads") && m == http.MethodGet:
		return "ListMultipartUploads"
	case hasQuery(r, "partNumber") && m == http.MethodPut:
		if r.Header.Get("x-amz-copy-source") != "" {
			return "UploadPartCopy"
		}
		return "UploadPart"
	case hasQuery(r, "uploadId") && m == http.MethodGet:
		return "ListParts"
	case hasQuery(r, "uploadId") && m == http.MethodPost:
		return "CompleteMultipartUpload"
	case hasQuery(r, "uploadId") && m == http.MethodDelete:
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
		if r.URL.Query().Get("list-type") == "1" {
			return "ListObjects"
		}
		return "ListObjectsV2"
	}
	return ""
}

func putOrGet(method, put, get string) string {
	return putGetDel(method, put, get, "")
}

func putGetDel(method, put, get, del string) string {
	switch method {
	case http.MethodPut:
		return put
	case http.MethodGet:
		return get
	case http.MethodDelete:
		if del != "" {
			return del
		}
	}
	return ""
}

func route53Op(svc *model.Service, r *http.Request) *model.Operation {
	if a := r.URL.Query().Get("Action"); a != "" {
		if op := svc.OperationByName(a); op != nil {
			return op
		}
		return &model.Operation{Name: a, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
	}
	path, m := r.URL.Path, r.Method
	name := "ListHostedZones"
	switch {
	case strings.Contains(path, "/rrset") && m == http.MethodPost:
		name = "ChangeResourceRecordSets"
	case strings.Contains(path, "/rrset") && m == http.MethodGet:
		name = "ListResourceRecordSets"
	case strings.HasSuffix(path, "/hostedzone") && m == http.MethodPost:
		name = "CreateHostedZone"
	case strings.HasSuffix(path, "/hostedzone") && m == http.MethodGet:
		name = "ListHostedZones"
	case m == http.MethodDelete:
		name = "DeleteHostedZone"
	case m == http.MethodGet:
		name = "GetHostedZone"
	}
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: m, Code: 200}}
}

func cloudfrontOp(svc *model.Service, r *http.Request) *model.Operation {
	if a := r.URL.Query().Get("Action"); a != "" {
		if op := svc.OperationByName(a); op != nil {
			return op
		}
		return &model.Operation{Name: a, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
	}
	path, m := r.URL.Path, r.Method
	name := "ListDistributions"
	switch {
	case strings.Contains(path, "/invalidation") && m == http.MethodPost:
		name = "CreateInvalidation"
	case strings.Contains(path, "/invalidation") && m == http.MethodGet:
		if strings.HasSuffix(path, "/invalidation") || strings.HasSuffix(path, "/invalidation/") {
			name = "ListInvalidations"
		} else {
			name = "GetInvalidation"
		}
	case strings.Contains(path, "/config") && m == http.MethodPut:
		name = "UpdateDistribution"
	case strings.Contains(path, "/config") && m == http.MethodGet:
		name = "GetDistributionConfig"
	case strings.HasSuffix(path, "/distribution") || strings.HasSuffix(path, "/distribution/"):
		if m == http.MethodPost {
			name = "CreateDistribution"
		} else {
			name = "ListDistributions"
		}
	case m == http.MethodDelete:
		name = "DeleteDistribution"
	case m == http.MethodGet:
		name = "GetDistribution"
	}
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: m, Code: 200}}
}

func azureOp(svc *model.Service, r *http.Request) *model.Operation {
	name := azureRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func azureRoute(r *http.Request) string {
	q := r.URL.Query()
	path := strings.Trim(r.URL.Path, "/")
	_, blob, _ := strings.Cut(path, "/")
	if path == "" {
		blob = ""
	}
	m := r.Method
	comp := q.Get("comp")
	restype := q.Get("restype")
	if restype == "account" && comp == "properties" {
		return "GetAccountInfo"
	}
	if restype == "service" {
		switch comp {
		case "properties":
			if m == http.MethodPut {
				return "SetServiceProperties"
			}
			return "GetServiceProperties"
		case "stats":
			return "GetServiceStats"
		}
	}
	if restype == "container" && blob == "" {
		switch comp {
		case "list":
			return "ListBlobs"
		case "blobs":
			return "FilterBlobs"
		case "batch":
			if m == http.MethodPost {
				return "SubmitBatch"
			}
			return "UnsupportedQuery"
		case "metadata":
			if m == http.MethodPut {
				return "SetContainerMetadata"
			}
			return "GetContainerMetadata"
		case "acl":
			if m == http.MethodPut {
				return "SetContainerAcl"
			}
			return "GetContainerAcl"
		case "lease":
			switch strings.ToLower(r.Header.Get("x-ms-lease-action")) {
			case "acquire":
				return "AcquireContainerLease"
			case "release":
				return "ReleaseContainerLease"
			case "renew":
				return "RenewContainerLease"
			case "break":
				return "BreakContainerLease"
			case "change":
				return "ChangeContainerLease"
			default:
				return "UnsupportedQuery"
			}
		case "":
			switch m {
			case http.MethodPut:
				return "CreateContainer"
			case http.MethodGet, http.MethodHead:
				return "GetContainer"
			case http.MethodDelete:
				return "DeleteContainer"
			}
		default:
			return "UnsupportedQuery"
		}
	}
	if path == "" {
		switch comp {
		case "list":
			return "ListContainers"
		case "blobs":
			return "FilterBlobs"
		case "batch":
			if m == http.MethodPost {
				return "SubmitBatch"
			}
			return "UnsupportedQuery"
		case "":
			return "Unknown"
		default:
			return "UnsupportedQuery"
		}
	}
	if blob != "" {
		switch comp {
		case "block":
			if r.Header.Get("x-ms-copy-source") != "" {
				return "StageBlockFromURL"
			}
			return "PutBlock"
		case "snapshot":
			if m == http.MethodPut {
				return "CreateSnapshot"
			}
			return "UnsupportedQuery"
		case "copy":
			if m == http.MethodPut && r.URL.Query().Get("copyid") != "" {
				return "AbortCopy"
			}
			return "UnsupportedQuery"
		case "appendblock":
			if r.Header.Get("x-ms-copy-source") != "" {
				return "AppendBlockFromURL"
			}
			return "AppendBlock"
		case "tier":
			if m == http.MethodPut {
				return "SetBlobTier"
			}
			return "UnsupportedQuery"
		case "lease":
			switch strings.ToLower(r.Header.Get("x-ms-lease-action")) {
			case "acquire":
				return "AcquireBlobLease"
			case "release":
				return "ReleaseBlobLease"
			case "renew":
				return "RenewBlobLease"
			case "break":
				return "BreakBlobLease"
			case "change":
				return "ChangeBlobLease"
			default:
				return "UnsupportedQuery"
			}
		case "tags":
			if m == http.MethodPut {
				return "SetTags"
			}
			return "GetTags"
		case "blocklist":
			if m == http.MethodGet || m == http.MethodHead {
				return "GetBlockList"
			}
			return "PutBlockList"
		case "metadata":
			if m == http.MethodPut {
				return "SetBlobMetadata"
			}
			return "GetBlobMetadata"
		case "properties":
			if r.Header.Get("x-ms-blob-content-length") != "" {
				return "ResizePageBlob"
			}
			if r.Header.Get("x-ms-sequence-number-action") != "" || r.Header.Get("x-ms-blob-sequence-number") != "" {
				return "SetBlobSequenceNumber"
			}
			return "SetBlobProperties"
		case "page":
			if m != http.MethodPut {
				return "UnsupportedQuery"
			}
			if r.Header.Get("x-ms-page-write") == "clear" {
				return "ClearPages"
			}
			if r.Header.Get("x-ms-copy-source") != "" {
				return "PutPageFromURL"
			}
			return "PutPage"
		case "pagelist":
			if m != http.MethodGet {
				return "UnsupportedQuery"
			}
			if q.Get("prevsnapshot") != "" {
				return "GetPageRangesDiff"
			}
			return "GetPageRanges"
		case "":
			switch m {
			case http.MethodPut:
				if r.Header.Get("x-ms-copy-source") != "" {
					if strings.EqualFold(r.Header.Get("x-ms-requires-sync"), "true") {
						return "CopyBlobFromURL"
					}
					return "StartCopyFromURL"
				}
				if strings.EqualFold(r.Header.Get("x-ms-blob-type"), "PageBlob") {
					return "CreatePageBlob"
				}
				if strings.EqualFold(r.Header.Get("x-ms-blob-type"), "AppendBlob") {
					return "CreateAppendBlob"
				}
				return "PutBlob"
			case http.MethodHead:
				return "GetBlobProperties"
			case http.MethodGet:
				return "GetBlob"
			case http.MethodDelete:
				return "DeleteBlob"
			}
		default:
			return "UnsupportedQuery"
		}
	}
	return "Unknown"
}

// azureETagList splits an If-Match/If-None-Match header into ETag tokens with
// surrounding quotes stripped (Azurite's ConditionalHeadersAdapter dequotes
// before comparing). "*" survives as a token.
func azureETagList(v string) []any {
	parts := strings.Split(v, ",")
	out := make([]any, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), `"`)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// azureParseTagsHeader parses the x-ms-tags header (URL query format
// k1=v1&k2=v2) into a tag map, or returns the Azurite validation error code.
func azureParseTagsHeader(v string) (map[string]any, string) {
	vals, err := url.ParseQuery(v)
	if err != nil {
		return nil, "DuplicateTagNames"
	}
	tags := map[string]any{}
	for k, vs := range vals {
		if len(vs) > 0 {
			tags[k] = vs[0]
		}
	}
	if code := azureValidateTags(tags); code != "" {
		return nil, code
	}
	return tags, ""
}

// azureValidateTags applies Azurite's Set Blob Tags limits in its order:
// count, then per-tag empty key, key length, value length, character set.
// The returned string is the Azurite error code (DuplicateTagNames is what
// Azurite's getInvalidTag really returns).
func azureValidateTags(tags map[string]any) string {
	if len(tags) > 10 {
		return "TagsTooLarge"
	}
	for k, v := range tags {
		if k == "" {
			return "EmptyTagName"
		}
		if len(k) > 128 || len(fmt.Sprint(v)) > 256 {
			return "TagsTooLarge"
		}
		if !azureValidTagChars(k) || !azureValidTagChars(fmt.Sprint(v)) {
			return "DuplicateTagNames"
		}
	}
	return ""
}

func azureValidTagChars(s string) bool {
	for _, c := range s {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == ' ' || c == '+' || c == '-' || c == '.' || c == '/' || c == ':' || c == '=' || c == '_'
		if !ok {
			return false
		}
	}
	return true
}

// azureValidTagExpr reports whether a tag-condition expression parses.
func azureValidTagExpr(s string) bool {
	_, err := bir.ParseTagExpr(s)
	return err == nil
}

func decodeAzureHeaders(in map[string]any, r *http.Request) {
	meta := map[string]any{}
	for k, vs := range r.Header {
		if len(vs) == 0 || vs[0] == "" {
			continue
		}
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-ms-meta-") {
			meta[strings.TrimPrefix(lk, "x-ms-meta-")] = vs[0]
			continue
		}
		switch lk {
		case "x-ms-blob-type":
			in["blob_type"] = vs[0]
		case "x-ms-lease-action":
			in["lease_action"] = vs[0]
		case "x-ms-lease-id":
			in["lease_id"] = vs[0]
		case "x-ms-proposed-lease-id":
			in["proposed_lease_id"] = vs[0]
		case "x-ms-copy-source":
			in["copy_source"] = vs[0]
		case "x-ms-range", "range":
			in["range"] = vs[0]
		case "x-ms-page-write":
			in["page_write"] = vs[0]
		case "x-ms-blob-public-access":
			in["public_access"] = vs[0]
		case "x-ms-lease-duration":
			in["lease_duration"] = vs[0]
		case "x-ms-lease-break-period":
			in["lease_break_period"] = vs[0]
		case "if-match":
			in["if_match_list"] = azureETagList(vs[0])
		case "if-none-match":
			in["if_none_match_list"] = azureETagList(vs[0])
		case "if-modified-since":
			if t, err := http.ParseTime(vs[0]); err == nil {
				in["if_modified_since_unix"] = t.Unix()
			}
		case "if-unmodified-since":
			if t, err := http.ParseTime(vs[0]); err == nil {
				in["if_unmodified_since_unix"] = t.Unix()
			}
		case "x-ms-if-sequence-number-eq":
			in["seq_eq"] = vs[0]
		case "x-ms-if-sequence-number-lt":
			in["seq_lt"] = vs[0]
		case "x-ms-if-sequence-number-le":
			in["seq_le"] = vs[0]
		case "x-ms-access-tier":
			in["access_tier"] = vs[0]
		case "x-ms-blob-condition-appendpos":
			in["append_pos"] = vs[0]
		case "x-ms-blob-condition-maxsize":
			in["max_size"] = vs[0]
		case "x-ms-tags":
			tags, code := azureParseTagsHeader(vs[0])
			if code != "" {
				in["tags_invalid"] = code
			} else {
				in["tags"] = tags
			}
		case "x-ms-if-tags":
			in["if_tags"] = vs[0]
			if !azureValidTagExpr(vs[0]) {
				in["if_tags_invalid"] = true
			}
		case "x-ms-source-if-tags":
			in["source_if_tags"] = vs[0]
			if !azureValidTagExpr(vs[0]) {
				in["source_if_tags_invalid"] = true
			}
		case "content-type":
			in["content_type"] = vs[0]
		case "x-ms-blob-cache-control":
			in["cache_control"] = vs[0]
		case "x-ms-blob-content-type":
			in["content_type"] = vs[0]
		case "x-ms-blob-content-md5":
			in["content_md5"] = vs[0]
		case "x-ms-blob-content-encoding":
			in["content_encoding"] = vs[0]
		case "x-ms-blob-content-language":
			in["content_language"] = vs[0]
		case "x-ms-blob-content-disposition":
			in["content_disposition"] = vs[0]
		case "x-ms-blob-content-length":
			in["content_length"] = vs[0]
		case "x-ms-blob-sequence-number":
			in["sequence_number"] = vs[0]
		case "x-ms-sequence-number-action":
			in["sequence_number_action"] = vs[0]
		case "x-ms-requires-sync":
			in["requires_sync"] = vs[0]
		case "x-ms-copy-action":
			in["copy_action"] = vs[0]
		case "x-ms-delete-snapshots":
			in["delete_snapshots"] = vs[0]
		case "x-ms-source-range":
			in["source_range"] = vs[0]
		}
	}
	if s, ok := in["range"].(string); ok {
		// "bytes=start-end" (a bare "-end" suffix range is left unparsed and
		// the operation's own requires decide what that means).
		var start, end int64
		if n, _ := fmt.Sscanf(s, "bytes=%d-%d", &start, &end); n == 2 {
			in["range_start"] = start
			in["range_end"] = end
		}
	}
	if s, ok := in["source_range"].(string); ok {
		var start, end int64
		if n, _ := fmt.Sscanf(s, "bytes=%d-%d", &start, &end); n == 2 {
			in["source_range_start"] = start
			in["source_range_end"] = end
		}
	}
	if cs, ok := in["copy_source"].(string); ok && cs != "" {
		// Azurite: a copy source that is not an absolute URL is 400
		// InvalidHeaderValue. The host is not checked -- same-instance
		// emulation reads every source from the local store.
		u, err := url.Parse(cs)
		sp := []string{}
		if err == nil && u.Host != "" {
			sp = strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
		}
		if len(sp) == 2 && sp[0] != "" && sp[1] != "" {
			in["source_container"] = sp[0]
			in["source_blob"] = sp[1]
			if s := u.Query().Get("snapshot"); s != "" {
				in["source_snapshot"] = s
			}
		} else {
			in["source_invalid"] = true
		}
	}
	if len(meta) > 0 {
		in["metadata"] = meta
	}
	if snap := r.URL.Query().Get("snapshot"); snap != "" {
		in["snapshot"] = snap
	}
}

// azureTagsXML is the Set/Get Blob Tags body shape.
type azureTagsXML struct {
	Tags []struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	} `xml:"TagSet>Tag"`
}

// azureParseTagsXML parses a Set Blob Tags body, or returns the Azurite
// validation error code (malformed XML is InvalidXmlDocument).
func azureParseTagsXML(body []byte) (map[string]any, string) {
	if len(bytes.TrimSpace(body)) == 0 {
		return map[string]any{}, ""
	}
	var doc azureTagsXML
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, "InvalidXmlDocument"
	}
	tags := map[string]any{}
	for _, t := range doc.Tags {
		tags[t.Key] = t.Value
	}
	if code := azureValidateTags(tags); code != "" {
		return nil, code
	}
	return tags, ""
}

func parseAzureBlockIDs(body []byte) []any {
	dec := xml.NewDecoder(bytes.NewReader(body))
	var ids []any
	var buf strings.Builder
	in := false
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "Latest", "Committed", "Uncommitted":
				in = true
				buf.Reset()
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "Latest", "Committed", "Uncommitted":
				if in {
					ids = append(ids, strings.TrimSpace(buf.String()))
					in = false
				}
			}
		case xml.CharData:
			if in {
				buf.Write(t)
			}
		}
	}
	return ids
}

func azureQueueOp(svc *model.Service, r *http.Request) *model.Operation {
	name := azureQueueRoute(r)
	if op := svc.OperationByName(name); op != nil {
		return op
	}
	return &model.Operation{Name: name, HTTP: model.HTTPBinding{Method: r.Method, Code: 200}}
}

func azureQueueRoute(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if path == "" {
		parts = nil
	}
	q := r.URL.Query()
	m := r.Method
	if path == "" {
		if q.Get("restype") == "service" {
			switch q.Get("comp") {
			case "properties":
				if m == http.MethodPut {
					return "SetServiceProperties"
				}
				return "GetServiceProperties"
			case "stats":
				return "GetServiceStats"
			}
		}
		if q.Get("comp") == "list" {
			return "ListQueues"
		}
		return "Unknown"
	}
	if len(parts) == 1 {
		switch q.Get("comp") {
		case "metadata":
			if m == http.MethodPut {
				return "SetQueueMetadata"
			}
			return "GetQueueProperties"
		case "acl":
			if m == http.MethodPut {
				return "SetQueueAcl"
			}
			return "GetQueueAcl"
		}
		switch m {
		case http.MethodPut:
			return "CreateQueue"
		case http.MethodDelete:
			return "DeleteQueue"
		}
	}
	if len(parts) >= 2 && parts[1] == "messages" {
		if len(parts) >= 3 {
			if m == http.MethodPut {
				return "UpdateMessage"
			}
			if m == http.MethodDelete {
				return "DeleteMessage"
			}
			return "Unknown"
		}
		switch m {
		case http.MethodPost:
			return "PutMessage"
		case http.MethodDelete:
			return "ClearMessages"
		case http.MethodGet:
			if q.Get("peekonly") == "true" {
				return "PeekMessages"
			}
			return "GetMessages"
		}
	}
	return "Unknown"
}

func decodeAzureQueue(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	in := map[string]any{}
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if path != "" {
		in["queue"] = parts[0]
		if len(parts) >= 3 {
			in["messageid"] = parts[2]
			in["id"] = parts[2]
		}
	}
	q := r.URL.Query()
	for _, k := range []string{"numofmessages", "visibilitytimeout", "messagettl", "popreceipt"} {
		if s := q.Get(k); s != "" {
			in[k] = s
		}
	}
	if strings.Contains(strings.ToLower(r.Host), "-secondary") {
		in["secondary"] = true
	}
	decodeAzureHeaders(in, r)
	req := &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}
	if r.Body != nil && (op.Name == "PutMessage" || op.Name == "UpdateMessage" || op.Name == "SetQueueAcl" || op.Name == "SetServiceProperties") {
		body, _ := io.ReadAll(r.Body)
		in["body"] = string(body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		if op.Name == "PutMessage" || op.Name == "UpdateMessage" {
			text, ok := azureMessageText(body)
			if !ok {
				in["body_invalid"] = true
			} else {
				in["message"] = text
			}
		}
		if op.Name == "SetQueueAcl" {
			in["acl"] = string(body)
		}
	}
	return req, nil
}

// azureMessageText pulls the MessageText element out of a QueueMessage
// envelope; the stored text is whatever the client encoded (Azurite keeps it
// verbatim, base64 or not).
func azureMessageText(body []byte) (string, bool) {
	if len(bytes.TrimSpace(body)) == 0 {
		return "", true
	}
	var doc struct {
		Text string `xml:"MessageText"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return "", false
	}
	return doc.Text, true
}

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	if svc.ID == "azure.queue" {
		return decodeAzureQueue(svc, op, r)
	}
	if svc.ID == "azure.blobs" {
		in := map[string]any{}
		path := strings.Trim(r.URL.Path, "/")
		container, blob, _ := strings.Cut(path, "/")
		if path != "" {
			in["container"] = container
			if blob != "" {
				in["blob"] = blob
			}
		}
		if bid := r.URL.Query().Get("blockid"); bid != "" {
			in["blockid"] = bid
		}
		if s := r.URL.Query().Get("snapshot"); s != "" {
			in["snapshot"] = s
		}
		if s := r.URL.Query().Get("copyid"); s != "" {
			in["copy_id"] = s
		}
		if s := r.URL.Query().Get("where"); s != "" {
			in["where"] = s
			if !azureValidTagExpr(s) {
				in["where_invalid"] = true
			}
		}
		if s := r.URL.Query().Get("prefix"); s != "" {
			in["prefix"] = s
		}
		if s := r.URL.Query().Get("delimiter"); s != "" {
			in["delimiter"] = s
		}
		if s := r.URL.Query().Get("include"); s != "" {
			in["include"] = s
		}
		if strings.Contains(strings.ToLower(r.Host), "-secondary") {
			in["secondary"] = true
		}
		decodeAzureHeaders(in, r)
		req := &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}
		if (op.Name == "PutBlob" || op.Name == "PutBlock" || op.Name == "PutBlockList" || op.Name == "AppendBlock" || op.Name == "SetContainerAcl" || op.Name == "SetServiceProperties" || op.Name == "PutPage") && r.Body != nil {
			body, _ := io.ReadAll(r.Body)
			in["body"] = string(body)
			if op.Name == "PutBlockList" {
				in["blockids"] = parseAzureBlockIDs(body)
			}
			if op.Name == "SetContainerAcl" {
				in["acl"] = string(body)
			}
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		if op.Name == "SetTags" && r.Body != nil {
			body, _ := io.ReadAll(r.Body)
			tags, code := azureParseTagsXML(body)
			if code != "" {
				in["tags_invalid"] = code
			} else {
				in["tags"] = tags
			}
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		return req, nil
	}
	if svc.ID == "aws.route53" {
		in := map[string]any{}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 3 && parts[1] == "hostedzone" {
			in["Id"] = parts[2]
		}
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			if len(b) > 0 {
				in["_body"] = string(b)
			}
		}
		return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
	}
	if svc.ID == "aws.cloudfront" {
		in := map[string]any{}
		for k, vs := range r.URL.Query() {
			if len(vs) > 0 {
				in[k] = vs[0]
			}
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		for i, p := range parts {
			if p == "distribution" && i+1 < len(parts) && parts[i+1] != "" {
				in["Id"] = parts[i+1]
			}
			if p == "invalidation" && i+1 < len(parts) && parts[i+1] != "" {
				in["InvalidationId"] = parts[i+1]
			}
		}
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			if len(b) > 0 {
				in["_body"] = string(b)
			}
		}
		return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
	}
	in := map[string]any{}
	bucket, key := bucketKey(r)
	if bucket != "" {
		in["Bucket"] = bucket
	}
	if key != "" {
		in["Key"] = key
	}
	for k, vs := range r.URL.Query() {
		in[k] = vs[0]
	}
	if versionID := r.URL.Query().Get("versionId"); versionID != "" {
		in["VersionId"] = versionID
	}
	if src := r.Header.Get("x-amz-copy-source"); src != "" {
		in["CopySource"] = strings.TrimPrefix(src, "/")
	}
	req := &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}
	streamOps := op.Name == "PutObject" || op.Name == "UploadPart" || op.Name == "PostObject"
	if r.Body != nil && streamOps {
		req.Body = r.Body
		return req, nil
	}
	if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		if len(b) > 0 {
			if op.Name == "DeleteObjects" {
				in["_body"] = string(b)
			}
			decodePayload(svc, op, b, in)
		}
	}
	if op.Name == "PutBucketLifecycleConfiguration" {
		if value := r.Header.Get("x-amz-transition-default-minimum-object-size"); value != "" {
			in["TransitionDefaultMinimumObjectSize"] = value
		}
	}
	return req, nil
}

type namedXMLNode struct {
	XMLName  xml.Name
	Attrs    []xml.Attr     `xml:",any,attr"`
	Text     string         `xml:",chardata"`
	Children []namedXMLNode `xml:",any"`
}

func parseNamedConfiguration(raw []byte, root string) (map[string]any, bool) {
	var document namedXMLNode
	if xml.Unmarshal(raw, &document) != nil || document.XMLName.Local != root {
		return nil, false
	}
	configuration, ok := namedXMLValue(document).(map[string]any)
	return configuration, ok
}

func namedXMLValue(node namedXMLNode) any {
	if len(node.Children) == 0 {
		value := strings.TrimSpace(node.Text)
		switch node.XMLName.Local {
		case "IsEnabled":
			parsed, _ := strconv.ParseBool(value)
			return parsed
		case "Days":
			parsed, _ := strconv.Atoi(value)
			return parsed
		}
		return value
	}
	if node.XMLName.Local == "OptionalFields" {
		fields := make([]any, 0, len(node.Children))
		for _, child := range node.Children {
			fields = append(fields, namedXMLValue(child))
		}
		return fields
	}
	result := map[string]any{}
	for _, child := range node.Children {
		key := child.XMLName.Local
		if key == "Tiering" || (key == "Tag" && node.XMLName.Local == "And") {
			key += "s"
		}
		value := namedXMLValue(child)
		if existing, ok := result[key]; ok {
			if values, ok := existing.([]any); ok {
				result[key] = append(values, value)
			} else {
				result[key] = []any{existing, value}
			}
		} else if key == "Tierings" || key == "Tags" {
			result[key] = []any{value}
		} else {
			result[key] = value
		}
	}
	return result
}

func (Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = op.HTTP.Code
		if status == 0 {
			status = 200
		}
	}
	if svc.ID == "azure.blobs" || svc.ID == "azure.queue" {
		return encodeAzure(w, status, resp, op.Name)
	}
	for k, vs := range resp.Headers {
		if strings.EqualFold(k, "ETag") {
			w.Header()["ETag"] = append(w.Header()["ETag"], vs...)
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if status == http.StatusNoContent {
		w.Header().Del("Content-Type")
		w.Header().Del("Content-Length")
		w.WriteHeader(status)
		return nil
	}
	if resp.Stream != nil {
		w.WriteHeader(status)
		_, err := io.Copy(w, resp.Stream)
		_ = resp.Stream.Close()
		return err
	}
	if op.Name == "UploadPart" && resp.Output == nil {
		w.Header().Del("Content-Type")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(status)
		return nil
	}
	if op.Name == "HeadObject" || op.Name == "HeadBucket" {
		w.WriteHeader(status)
		return nil
	}
	if op.Name == "GetBucketPolicy" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, err := io.WriteString(w, fmt.Sprint(resp.Output["Policy"]))
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	// An operation whose output is Unit has no body, whatever the answer
	// carries: an engine answers {} where a pack answered nil.
	if resp.Output == nil || op.Output == "smithy.api#Unit" {
		return nil
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	e := xmlenc.Encoder{Svc: svc}
	out := svc.Shapes[op.Output]
	switch name, member, ok := payloadOf(svc, op.Output); {
	case ok:
		// The body is one member, under its own element. A pack answers it
		// either under that name or flat, and an empty answer is an empty
		// body -- a bucket with no encryption configuration answers nothing.
		if len(resp.Output) == 0 {
			return nil
		}
		v, present := resp.Output[name]
		if !present {
			v = resp.Output
		}
		wire := wireName(name, member.Binding)
		fmt.Fprintf(&b, `<%s xmlns=%q>`, wire, svc.XMLNamespace)
		e.Value(&b, member.Shape, v)
		fmt.Fprintf(&b, "</%s>", wire)
	case op.Name == "GetBucketLocation":
		// aws.customizations#s3UnwrappedXmlOutput: the one member is the
		// root, not a child of it.
		fmt.Fprintf(&b, `<LocationConstraint xmlns=%q>%s</LocationConstraint>`, svc.XMLNamespace, xmlenc.Escape(fmt.Sprint(resp.Output["LocationConstraint"])))
	default:
		// The root is the output shape's own xmlName -- ListBucketResult,
		// AccessControlPolicy -- and, as restXml has it, the shape's own name
		// where the trait is absent: GetBucketNotificationConfiguration's
		// output is the NotificationConfiguration structure itself.
		root := out.XMLName
		if root == "" {
			root = op.Output[strings.LastIndex(op.Output, "#")+1:]
		}
		if op.Output == "" {
			root = op.Name + "Result"
		}
		if op.Name == "PostObject" {
			root = "PostResponse" // no model describes the browser upload
		}
		// The namespace is the model's; an operation the model does not
		// describe answers without one.
		if op.Output == "" {
			fmt.Fprintf(&b, "<%s>", root)
		} else {
			fmt.Fprintf(&b, `<%s xmlns=%q>`, root, svc.XMLNamespace)
		}
		e.Value(&b, op.Output, resp.Output)
		fmt.Fprintf(&b, "</%s>", root)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

type namedConfigurationXMLShape struct{ configuration, list string }

func namedConfigurationShape(operation string) namedConfigurationXMLShape {
	if !strings.Contains(operation, "Bucket") || (!strings.HasPrefix(operation, "Put") && !strings.HasPrefix(operation, "Get") && !strings.HasPrefix(operation, "List")) {
		return namedConfigurationXMLShape{}
	}
	switch {
	case strings.Contains(operation, "AnalyticsConfiguration"):
		return namedConfigurationXMLShape{"AnalyticsConfiguration", "AnalyticsConfigurationList"}
	case strings.Contains(operation, "InventoryConfiguration"):
		return namedConfigurationXMLShape{"InventoryConfiguration", "InventoryConfigurationList"}
	case strings.Contains(operation, "IntelligentTieringConfiguration"):
		return namedConfigurationXMLShape{"IntelligentTieringConfiguration", "IntelligentTieringConfigurationList"}
	case strings.Contains(operation, "MetricsConfiguration"):
		return namedConfigurationXMLShape{"MetricsConfiguration", "MetricsConfigurationList"}
	default:
		return namedConfigurationXMLShape{}
	}
}

const defaultAzureServiceProperties = `<?xml version="1.0" encoding="utf-8"?><StorageServiceProperties><Logging><Version>1.0</Version><Delete>false</Delete><Read>false</Read><Write>false</Write><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging><HourMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics><MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics><Cors /><DefaultServiceVersion>2026-06-06</DefaultServiceVersion><DeleteRetentionPolicy><Enabled>false</Enabled></DeleteRetentionPolicy></StorageServiceProperties>`

func encodeAzure(w http.ResponseWriter, status int, resp *spi.Response, op string) error {
	for k, vs := range resp.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	writeAzureContainerHeaders(w, resp)
	if resp.Stream != nil {
		w.WriteHeader(status)
		_, err := io.Copy(w, resp.Stream)
		_ = resp.Stream.Close()
		return err
	}
	if resp != nil && resp.Output != nil && op == "GetServiceProperties" {
		body := strAny(resp.Output["properties"])
		if body == "" {
			body = defaultAzureServiceProperties
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, body)
		return err
	}
	if resp != nil && resp.Output != nil && op == "GetServiceStats" {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><StorageServiceStats><GeoReplication><Status>live</Status></GeoReplication></StorageServiceStats>`)
		return err
	}
	if resp != nil && resp.Output != nil && op == "GetAccountInfo" {
		if k := strAny(resp.Output["account_kind"]); k != "" {
			w.Header().Set("x-ms-account-kind", k)
		}
		if s := strAny(resp.Output["sku_name"]); s != "" {
			w.Header().Set("x-ms-sku-name", s)
		}
		if h := strAny(resp.Output["hns"]); h != "" {
			w.Header().Set("x-ms-is-hns-enabled", h)
		}
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && (op == "GetBlobProperties" || op == "GetBlobMetadata" || op == "SetBlobMetadata" || op == "SetBlobProperties") {
		writeAzureBlobHeaders(w, resp)
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && op == "AppendBlock" {
		w.Header().Set("x-ms-blob-append-offset", strAny(resp.Output["append_offset"]))
		w.Header().Set("x-ms-blob-committed-block-count", strAny(resp.Output["committed_block_count"]))
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && op == "CreateSnapshot" {
		w.Header().Set("x-ms-snapshot", strAny(resp.Output["snapshot"]))
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && (op == "StartCopyFromURL" || op == "CopyBlobFromURL") {
		w.Header().Set("x-ms-copy-id", strAny(resp.Output["copy_id"]))
		w.Header().Set("x-ms-copy-status", strAny(resp.Output["copy_status"]))
		if op == "CopyBlobFromURL" {
			if s := strAny(resp.Output["content_md5"]); s != "" {
				w.Header().Set("Content-MD5", s)
			}
		}
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && op == "GetPageRanges" {
		b := &strings.Builder{}
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?><PageList>`)
		if ranges, ok := resp.Output["ranges"].([]any); ok {
			for _, r := range ranges {
				if m, ok := r.(map[string]any); ok {
					fmt.Fprintf(b, "<PageRange><Start>%v</Start><End>%v</End></PageRange>", m["start"], m["end"])
				}
			}
		}
		b.WriteString(`</PageList>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, b.String())
		return err
	}
	if resp != nil && resp.Output != nil && op == "GetTags" {
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?><Tags><TagSet>`)
		writeAzureTagSet(&b, resp.Output["tags"])
		b.WriteString(`</TagSet></Tags>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, b.String())
		return err
	}
	if resp != nil && resp.Output != nil && op == "FilterBlobs" {
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="">`)
		b.WriteString("<Where>")
		b.WriteString(xmlenc.Escape(strAny(resp.Output["where"])))
		b.WriteString("</Where><Blobs>")
		if lst, ok := resp.Output["blobs"].([]any); ok {
			for _, item := range lst {
				m, _ := item.(map[string]any)
				b.WriteString("<Blob><Name>")
				b.WriteString(xmlenc.Escape(strAny(m["name"])))
				b.WriteString("</Name><ContainerName>")
				b.WriteString(xmlenc.Escape(strAny(m["container"])))
				b.WriteString("</ContainerName><Tags><TagSet>")
				writeAzureTagSet(&b, m["tags"])
				b.WriteString("</TagSet></Tags></Blob>")
			}
		}
		b.WriteString(`</Blobs><NextMarker></NextMarker></EnumerationResults>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, b.String())
		return err
	}
	if resp != nil && resp.Output != nil && op == "PutMessage" {
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="utf-8"?><QueueMessagesList>`)
		writeAzureQueueMessage(&b, resp.Output, true)
		b.WriteString("</QueueMessagesList>")
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, b.String())
		return err
	}
	if resp != nil && resp.Output != nil && op == "UpdateMessage" {
		w.Header().Set("x-ms-popreceipt", strAny(resp.Output["pop_receipt"]))
		if s := azureUnix(resp.Output["visible_at"]); s != "" {
			w.Header().Set("x-ms-time-next-visible", s)
		}
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && op == "GetQueueProperties" {
		if s := strAny(resp.Output["approx_count"]); s != "" {
			w.Header().Set("x-ms-approximate-messages-count", s)
		}
		w.WriteHeader(status)
		return nil
	}
	if resp != nil && resp.Output != nil && (op == "GetContainerAcl" || op == "GetQueueAcl") {
		body := strAny(resp.Output["acl"])
		if body == "" {
			body = `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers></SignedIdentifiers>`
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, body)
		return err
	}
	if resp != nil && resp.Output != nil {
		if raw, ok := resp.Output["_raw"].(string); ok {
			if w.Header().Get("Content-Type") == "" {
				if op == "GetContainerAcl" {
					w.Header().Set("Content-Type", "application/xml")
				} else {
					w.Header().Set("Content-Type", "application/octet-stream")
				}
			}
			w.WriteHeader(status)
			_, err := io.WriteString(w, raw)
			return err
		}
	}
	if resp.Output != nil {
		if lst, ok := resp.Output["_list"]; ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(status)
			if op == "GetMessages" || op == "PeekMessages" {
				var b strings.Builder
				b.WriteString(`<?xml version="1.0" encoding="utf-8"?><QueueMessagesList>`)
				for _, item := range asAny(lst) {
					m, _ := item.(map[string]any)
					writeAzureQueueMessage(&b, m, op == "GetMessages")
				}
				b.WriteString("</QueueMessagesList>")
				_, err := io.WriteString(w, b.String())
				return err
			}
			if op == "GetBlockList" {
				var b strings.Builder
				b.WriteString(`<?xml version="1.0" encoding="utf-8"?><BlockList>`)
				for _, item := range asAny(lst) {
					m, _ := item.(map[string]any)
					b.WriteString("<Latest>")
					b.WriteString(xmlenc.Escape(strAny(m["id"])))
					b.WriteString("</Latest>")
				}
				b.WriteString("</BlockList>")
				_, err := io.WriteString(w, b.String())
				return err
			}
			kind, _ := resp.Output["_kind"].(string)
			if kind == "" && op == "ListBlobs" {
				kind = "blobs"
			}
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults>`)
			if kind == "blobs" {
				b.WriteString("<Blobs>")
				for _, item := range asAny(lst) {
					m, _ := item.(map[string]any)
					if p := strAny(m["prefix"]); p != "" {
						b.WriteString("<BlobPrefix><Name>")
						b.WriteString(xmlenc.Escape(p))
						b.WriteString("</Name></BlobPrefix>")
						continue
					}
					writeAzureBlobListItem(&b, m, "")
					if snaps, ok := m["snapshots"].([]any); ok {
						for _, s := range snaps {
							sm, _ := s.(map[string]any)
							writeAzureBlobListItem(&b, m, strAny(sm["id"]))
						}
					}
				}
				b.WriteString("</Blobs>")
			} else {
				b.WriteString("<Containers>")
				for _, item := range asAny(lst) {
					m, _ := item.(map[string]any)
					b.WriteString("<Container><Name>")
					b.WriteString(xmlenc.Escape(strAny(m["name"])))
					b.WriteString("</Name></Container>")
				}
				b.WriteString("</Containers>")
			}
			b.WriteString("</EnumerationResults>")
			_, err := io.WriteString(w, b.String())
			return err
		}
	}
	w.WriteHeader(status)
	return nil
}

// writeAzureBlobListItem writes one <Blob> entry; snapshot carries the
// snapshot id when the entry is a snapshot projection.
func writeAzureBlobListItem(b *strings.Builder, m map[string]any, snapshot string) {
	b.WriteString("<Blob><Name>")
	b.WriteString(xmlenc.Escape(strAny(m["name"])))
	b.WriteString("</Name>")
	if snapshot != "" {
		b.WriteString("<Snapshot>")
		b.WriteString(xmlenc.Escape(snapshot))
		b.WriteString("</Snapshot>")
	}
	if tags, ok := m["tags"].(map[string]any); ok && len(tags) > 0 {
		b.WriteString("<Tags><TagSet>")
		writeAzureTagSet(b, tags)
		b.WriteString("</TagSet></Tags>")
	}
	b.WriteString("</Blob>")
}

// writeAzureTagSet writes <Tag><Key/><Value/></Tag> entries, key-sorted for
// deterministic output.
func writeAzureTagSet(b *strings.Builder, v any) {
	tags, ok := v.(map[string]any)
	if !ok {
		return
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString("<Tag><Key>")
		b.WriteString(xmlenc.Escape(k))
		b.WriteString("</Key><Value>")
		b.WriteString(xmlenc.Escape(fmt.Sprint(tags[k])))
		b.WriteString("</Value></Tag>")
	}
}

// azureUnix renders a unix-seconds member as an RFC1123 GMT string.
func azureUnix(v any) string {
	var n int64
	switch t := v.(type) {
	case int64:
		n = t
	case int:
		n = int64(t)
	case float64:
		n = int64(t)
	case string:
		n, _ = strconv.ParseInt(t, 10, 64)
	}
	if n <= 0 {
		return ""
	}
	return time.Unix(n, 0).UTC().Format(http.TimeFormat)
}

// writeAzureQueueMessage writes one <QueueMessage>; dequeue responses carry
// PopReceipt and TimeNextVisible, peek responses do not.
func writeAzureQueueMessage(b *strings.Builder, m map[string]any, withReceipt bool) {
	b.WriteString("<QueueMessage><MessageId>")
	b.WriteString(xmlenc.Escape(strAny(m["id"])))
	b.WriteString("</MessageId>")
	if s := azureUnix(m["inserted_at"]); s != "" {
		b.WriteString("<InsertionTime>")
		b.WriteString(s)
		b.WriteString("</InsertionTime>")
	}
	if s := azureUnix(m["expires_at"]); s != "" {
		b.WriteString("<ExpirationTime>")
		b.WriteString(s)
		b.WriteString("</ExpirationTime>")
	}
	if withReceipt {
		if s := strAny(m["pop_receipt"]); s != "" {
			b.WriteString("<PopReceipt>")
			b.WriteString(xmlenc.Escape(s))
			b.WriteString("</PopReceipt>")
		}
		if s := azureUnix(m["visible_at"]); s != "" {
			b.WriteString("<TimeNextVisible>")
			b.WriteString(s)
			b.WriteString("</TimeNextVisible>")
		}
	}
	b.WriteString("<DequeueCount>")
	b.WriteString(strAny(m["dequeue_count"]))
	b.WriteString("</DequeueCount><MessageText>")
	b.WriteString(xmlenc.Escape(strAny(m["message"])))
	b.WriteString("</MessageText></QueueMessage>")
}

func writeAzureBlobHeaders(w http.ResponseWriter, resp *spi.Response) {
	if resp == nil || resp.Output == nil {
		return
	}
	out := resp.Output
	if meta, ok := out["metadata"].(map[string]any); ok {
		for k, v := range meta {
			if k == "" || v == nil {
				continue
			}
			w.Header().Set("x-ms-meta-"+k, fmt.Sprint(v))
		}
	}
	if s := strAny(out["content_type"]); s != "" {
		w.Header().Set("Content-Type", s)
	}
	if s := strAny(out["cache_control"]); s != "" {
		w.Header().Set("Cache-Control", s)
	}
	if s := strAny(out["content_encoding"]); s != "" {
		w.Header().Set("Content-Encoding", s)
	}
	if s := strAny(out["content_language"]); s != "" {
		w.Header().Set("Content-Language", s)
	}
	if s := strAny(out["content_disposition"]); s != "" {
		w.Header().Set("Content-Disposition", s)
	}
	if s := strAny(out["content_md5"]); s != "" {
		w.Header().Set("Content-MD5", s)
	}
	if s := strAny(out["blob_type"]); s != "" {
		w.Header().Set("x-ms-blob-type", s)
	}
	if s := strAny(out["sequence_number"]); s != "" {
		w.Header().Set("x-ms-blob-sequence-number", s)
	}
	if s := strAny(out["content_length"]); s != "" {
		w.Header().Set("Content-Length", s)
	}
	if s := strAny(out["tag_count"]); s != "" {
		w.Header().Set("x-ms-tag-count", s)
	}
	if s := strAny(out["access_tier"]); s != "" {
		w.Header().Set("x-ms-access-tier", s)
	}
	if s := strAny(out["access_tier_inferred"]); s != "" {
		w.Header().Set("x-ms-access-tier-inferred", s)
	}
	writeAzureEntityHeaders(w, out)
}

// writeAzureEntityHeaders emits ETag and Last-Modified from output members;
// last_modified is unix seconds as a string.
func writeAzureEntityHeaders(w http.ResponseWriter, out map[string]any) {
	if s := strAny(out["etag"]); s != "" {
		w.Header().Set("ETag", s)
	}
	if s := strAny(out["last_modified"]); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 {
			w.Header().Set("Last-Modified", time.Unix(n, 0).UTC().Format(http.TimeFormat))
		}
	}
}

func writeAzureContainerHeaders(w http.ResponseWriter, resp *spi.Response) {
	if resp == nil || resp.Output == nil {
		return
	}
	out := resp.Output
	if meta, ok := out["metadata"].(map[string]any); ok {
		for k, v := range meta {
			if k == "" || v == nil {
				continue
			}
			w.Header().Set("x-ms-meta-"+k, fmt.Sprint(v))
		}
	}
	if id := strAny(out["lease_id"]); id != "" {
		w.Header().Set("x-ms-lease-id", id)
	}
	if s := strAny(out["lease_status"]); s != "" {
		w.Header().Set("x-ms-lease-status", s)
	}
	if s := strAny(out["lease_state"]); s != "" {
		w.Header().Set("x-ms-lease-state", s)
	}
	if s := strAny(out["lease_duration"]); s != "" {
		w.Header().Set("x-ms-lease-duration", s)
	}
	if s := strAny(out["public_access"]); s != "" {
		w.Header().Set("x-ms-blob-public-access", s)
	}
}

func asAny(v any) []any {
	s, _ := v.([]any)
	return s
}

func strAny(v any) string {
	s, _ := v.(string)
	return s
}

func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		status = 400
	}
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	if status == 304 {
		// Conditional read miss: no body, no x-ms-error-code.
		w.WriteHeader(status)
		return nil
	}
	if svc.ID == "azure.blobs" || svc.ID == "azure.queue" {
		w.Header().Set("x-ms-error-code", f.Code)
		if op != nil && op.Name == "GetBlobProperties" {
			w.WriteHeader(status)
			return nil
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, err := io.WriteString(w, `<Error><Code>`+xmlenc.Escape(f.Code)+`</Code><Message>`+xmlenc.Escape(f.Message)+`</Message></Error>`)
		return err
	}
	if region, _ := f.Fields["Region"].(string); region != "" {
		w.Header().Set("x-amz-bucket-region", region)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	message := f.Message
	if message == "" && f.Code == "MalformedXML" {
		message = "The XML you provided was not well-formed or did not validate against our published schema"
	}
	var body strings.Builder
	fmt.Fprintf(&body, `<Error><Code>%s</Code><Message>%s</Message>`, xmlenc.Escape(f.Code), xmlenc.Escape(message))
	xmlenc.Encoder{}.Value(&body, "", f.Fields)
	fmt.Fprintf(&body, `<RequestId>%s</RequestId><HostId>mirror</HostId></Error>`, xmlenc.Escape(requestID))
	_, err := io.WriteString(w, body.String())
	return err
}
