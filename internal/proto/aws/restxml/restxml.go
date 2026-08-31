// Package restxml implements the S3 restXml codec.
package restxml

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
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

// RouteName maps an S3 REST request to the Smithy operation name.
// Query flags are matched by key presence (`?tagging`, `?delete`), not Get() != "".
func RouteName(r *http.Request) string {
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
		return putOrGet(m, "PutObjectLockConfiguration", "GetBucketObjectLockConfiguration")
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

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
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
			parseXMLInput(op.Name, b, in)
		}
	}
	if op.Name == "PutBucketLifecycleConfiguration" {
		if value := r.Header.Get("x-amz-transition-default-minimum-object-size"); value != "" {
			in["TransitionDefaultMinimumObjectSize"] = value
		}
	}
	return req, nil
}

func parseXMLInput(op string, raw []byte, in map[string]any) {
	switch op {
	case "PutBucketAnalyticsConfiguration", "PutBucketInventoryConfiguration", "PutBucketIntelligentTieringConfiguration", "PutBucketMetricsConfiguration":
		configuration, ok := parseNamedConfiguration(raw, namedConfigurationShape(op).configuration)
		if !ok {
			in["_body"] = string(raw)
			return
		}
		in[namedConfigurationShape(op).configuration] = configuration
	case "PutBucketAcl", "PutObjectAcl":
		var policy struct {
			XMLName xml.Name
			Owner   *struct {
				ID          string `xml:"ID"`
				DisplayName string `xml:"DisplayName"`
			} `xml:"Owner"`
			AccessControlList *struct {
				Grants []struct {
					Grantee struct {
						Type         string `xml:"type,attr"`
						ID           string `xml:"ID"`
						DisplayName  string `xml:"DisplayName"`
						URI          string `xml:"URI"`
						EmailAddress string `xml:"EmailAddress"`
					} `xml:"Grantee"`
					Permission string `xml:"Permission"`
				} `xml:"Grant"`
			} `xml:"AccessControlList"`
		}
		if xml.Unmarshal(raw, &policy) != nil || policy.XMLName.Local != "AccessControlPolicy" {
			in["_body"] = string(raw)
			return
		}
		document := map[string]any{}
		if policy.Owner != nil {
			document["Owner"] = map[string]any{"ID": policy.Owner.ID, "DisplayName": policy.Owner.DisplayName}
		}
		if policy.AccessControlList != nil {
			grants := make([]any, 0, len(policy.AccessControlList.Grants))
			for _, grant := range policy.AccessControlList.Grants {
				grantee := map[string]any{"Type": grant.Grantee.Type}
				for field, value := range map[string]string{"ID": grant.Grantee.ID, "DisplayName": grant.Grantee.DisplayName, "URI": grant.Grantee.URI, "EmailAddress": grant.Grantee.EmailAddress} {
					if value != "" {
						grantee[field] = value
					}
				}
				grants = append(grants, map[string]any{"Grantee": grantee, "Permission": grant.Permission})
			}
			document["Grants"] = grants
		}
		in["AccessControlPolicy"] = document
	case "CreateBucket":
		var cfg struct {
			LocationConstraint string `xml:"LocationConstraint"`
			Tags               *struct {
				Tag []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"Tag"`
			} `xml:"Tags"`
		}
		if xml.Unmarshal(raw, &cfg) != nil {
			in["_body"] = string(raw)
			return
		}
		in["LocationConstraint"] = cfg.LocationConstraint
		configuration := map[string]any{"LocationConstraint": cfg.LocationConstraint}
		if cfg.Tags != nil {
			tags := make([]any, 0, len(cfg.Tags.Tag))
			for _, tag := range cfg.Tags.Tag {
				tags = append(tags, map[string]any{"Key": tag.Key, "Value": tag.Value})
			}
			configuration["Tags"] = tags
		}
		in["CreateBucketConfiguration"] = configuration
	case "DeleteObjects":
		var d struct {
			Object []struct {
				Key       string `xml:"Key"`
				VersionID string `xml:"VersionId"`
			} `xml:"Object"`
			Quiet bool `xml:"Quiet"`
		}
		if xml.Unmarshal(raw, &d) != nil {
			in["_body"] = string(raw)
			return
		}
		objs := make([]any, 0, len(d.Object))
		for _, o := range d.Object {
			item := map[string]any{"Key": o.Key}
			if o.VersionID != "" {
				item["VersionId"] = o.VersionID
			}
			objs = append(objs, item)
		}
		in["Objects"] = objs
		in["Quiet"] = d.Quiet
		in["Delete"] = map[string]any{"Objects": objs, "Quiet": d.Quiet}
	case "CompleteMultipartUpload":
		var completed struct {
			Part []struct {
				ETag       string `xml:"ETag"`
				PartNumber int    `xml:"PartNumber"`
			} `xml:"Part"`
		}
		if xml.Unmarshal(raw, &completed) != nil {
			in["_body"] = string(raw)
			return
		}
		parts := make([]any, 0, len(completed.Part))
		for _, part := range completed.Part {
			parts = append(parts, map[string]any{"ETag": part.ETag, "PartNumber": part.PartNumber})
		}
		in["MultipartUpload"] = map[string]any{"Parts": parts}
	case "RestoreObject":
		var restore struct {
			Days int `xml:"Days"`
		}
		if xml.Unmarshal(raw, &restore) != nil {
			in["_body"] = string(raw)
			return
		}
		in["Days"] = restore.Days
		in["RestoreRequest"] = map[string]any{"Days": restore.Days}
	case "PutBucketTagging", "PutObjectTagging":
		var t struct {
			TagSet *struct {
				Tag []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"Tag"`
			} `xml:"TagSet"`
		}
		if xml.Unmarshal(raw, &t) != nil {
			in["_body"] = string(raw)
			return
		}
		if t.TagSet == nil {
			return
		}
		tags := make([]any, 0, len(t.TagSet.Tag))
		for _, tg := range t.TagSet.Tag {
			tags = append(tags, map[string]any{"Key": tg.Key, "Value": tg.Value})
		}
		in["TagSet"] = tags
	case "PutBucketOwnershipControls":
		var controls struct {
			Rules []struct {
				ObjectOwnership string `xml:"ObjectOwnership"`
			} `xml:"Rule"`
		}
		if xml.Unmarshal(raw, &controls) != nil {
			in["_body"] = string(raw)
			return
		}
		rules := make([]any, 0, len(controls.Rules))
		for _, rule := range controls.Rules {
			rules = append(rules, map[string]any{"ObjectOwnership": rule.ObjectOwnership})
		}
		in["OwnershipControls"] = map[string]any{"Rules": rules}
	case "PutPublicAccessBlock":
		var configuration struct {
			XMLName               xml.Name
			BlockPublicAcls       *bool                        `xml:"BlockPublicAcls"`
			BlockPublicPolicy     *bool                        `xml:"BlockPublicPolicy"`
			IgnorePublicAcls      *bool                        `xml:"IgnorePublicAcls"`
			RestrictPublicBuckets *bool                        `xml:"RestrictPublicBuckets"`
			Unknown               []struct{ XMLName xml.Name } `xml:",any"`
		}
		if xml.Unmarshal(raw, &configuration) != nil || configuration.XMLName.Local != "PublicAccessBlockConfiguration" {
			in["_body"] = string(raw)
			return
		}
		publicAccessBlock := map[string]any{}
		for field, value := range map[string]*bool{
			"BlockPublicAcls": configuration.BlockPublicAcls, "BlockPublicPolicy": configuration.BlockPublicPolicy,
			"IgnorePublicAcls": configuration.IgnorePublicAcls, "RestrictPublicBuckets": configuration.RestrictPublicBuckets,
		} {
			if value != nil {
				publicAccessBlock[field] = *value
			}
		}
		for _, field := range configuration.Unknown {
			publicAccessBlock[field.XMLName.Local] = nil
		}
		in["PublicAccessBlockConfiguration"] = publicAccessBlock
	case "PutBucketRequestPayment":
		var configuration struct {
			XMLName xml.Name
			Payer   string `xml:"Payer"`
		}
		if xml.Unmarshal(raw, &configuration) != nil || configuration.XMLName.Local != "RequestPaymentConfiguration" {
			in["_body"] = string(raw)
			return
		}
		in["RequestPaymentConfiguration"] = map[string]any{"Payer": configuration.Payer}
	case "PutBucketAccelerateConfiguration":
		var configuration struct {
			XMLName xml.Name
			Status  string `xml:"Status"`
		}
		if xml.Unmarshal(raw, &configuration) != nil || configuration.XMLName.Local != "AccelerateConfiguration" {
			in["_body"] = string(raw)
			return
		}
		in["AccelerateConfiguration"] = map[string]any{"Status": configuration.Status}
	case "PutBucketLogging":
		var status struct {
			XMLName        xml.Name
			LoggingEnabled *struct {
				TargetBucket string `xml:"TargetBucket"`
				TargetPrefix string `xml:"TargetPrefix"`
				TargetGrants []struct {
					Grantee struct {
						DisplayName  string `xml:"DisplayName"`
						EmailAddress string `xml:"EmailAddress"`
						ID           string `xml:"ID"`
						URI          string `xml:"URI"`
						Type         string `xml:"type,attr"`
					} `xml:"Grantee"`
					Permission string `xml:"Permission"`
				} `xml:"TargetGrants>Grant"`
				TargetObjectKeyFormat *struct {
					SimplePrefix      *struct{} `xml:"SimplePrefix"`
					PartitionedPrefix *struct {
						PartitionDateSource string `xml:"PartitionDateSource"`
					} `xml:"PartitionedPrefix"`
				} `xml:"TargetObjectKeyFormat"`
			} `xml:"LoggingEnabled"`
		}
		if xml.Unmarshal(raw, &status) != nil || status.XMLName.Local != "BucketLoggingStatus" {
			in["_body"] = string(raw)
			return
		}
		document := map[string]any{}
		if source := status.LoggingEnabled; source != nil {
			logging := map[string]any{"TargetBucket": source.TargetBucket, "TargetPrefix": source.TargetPrefix}
			if len(source.TargetGrants) != 0 {
				grants := make([]any, 0, len(source.TargetGrants))
				for _, source := range source.TargetGrants {
					grantee := map[string]any{}
					for key, value := range map[string]string{"DisplayName": source.Grantee.DisplayName, "EmailAddress": source.Grantee.EmailAddress, "ID": source.Grantee.ID, "Type": source.Grantee.Type, "URI": source.Grantee.URI} {
						if value != "" {
							grantee[key] = value
						}
					}
					grants = append(grants, map[string]any{"Grantee": grantee, "Permission": source.Permission})
				}
				logging["TargetGrants"] = grants
			}
			if format := source.TargetObjectKeyFormat; format != nil {
				value := map[string]any{}
				if format.SimplePrefix != nil {
					value["SimplePrefix"] = map[string]any{}
				}
				if format.PartitionedPrefix != nil {
					value["PartitionedPrefix"] = map[string]any{"PartitionDateSource": format.PartitionedPrefix.PartitionDateSource}
				}
				logging["TargetObjectKeyFormat"] = value
			}
			document["LoggingEnabled"] = logging
		}
		in["BucketLoggingStatus"] = document
	case "PutBucketCors":
		var configuration struct {
			XMLName xml.Name
			Rules   []struct {
				AllowedHeaders []string `xml:"AllowedHeader"`
				AllowedMethods []string `xml:"AllowedMethod"`
				AllowedOrigins []string `xml:"AllowedOrigin"`
				ExposeHeaders  []string `xml:"ExposeHeader"`
				MaxAgeSeconds  *int     `xml:"MaxAgeSeconds"`
				ID             string   `xml:"ID"`
			} `xml:"CORSRule"`
		}
		if xml.Unmarshal(raw, &configuration) != nil || configuration.XMLName.Local != "CORSConfiguration" {
			in["_body"] = string(raw)
			return
		}
		rules := make([]any, 0, len(configuration.Rules))
		for _, source := range configuration.Rules {
			rule := map[string]any{"AllowedMethods": stringsToAny(source.AllowedMethods), "AllowedOrigins": stringsToAny(source.AllowedOrigins)}
			for key, values := range map[string][]string{"AllowedHeaders": source.AllowedHeaders, "ExposeHeaders": source.ExposeHeaders} {
				if len(values) != 0 {
					rule[key] = stringsToAny(values)
				}
			}
			if source.MaxAgeSeconds != nil {
				rule["MaxAgeSeconds"] = *source.MaxAgeSeconds
			}
			if source.ID != "" {
				rule["ID"] = source.ID
			}
			rules = append(rules, rule)
		}
		in["CORSConfiguration"] = map[string]any{"CORSRules": rules}
	case "PutBucketWebsite":
		type redirect struct {
			HostName             *string `xml:"HostName"`
			HTTPRedirectCode     string  `xml:"HttpRedirectCode"`
			Protocol             string  `xml:"Protocol"`
			ReplaceKeyPrefixWith *string `xml:"ReplaceKeyPrefixWith"`
			ReplaceKeyWith       *string `xml:"ReplaceKeyWith"`
		}
		var configuration struct {
			XMLName               xml.Name
			RedirectAllRequestsTo *redirect `xml:"RedirectAllRequestsTo"`
			IndexDocument         *struct {
				Suffix *string `xml:"Suffix"`
			} `xml:"IndexDocument"`
			ErrorDocument *struct {
				Key *string `xml:"Key"`
			} `xml:"ErrorDocument"`
			RoutingRules *struct {
				Rules []struct {
					Condition *struct {
						HTTPErrorCodeReturnedEquals string `xml:"HttpErrorCodeReturnedEquals"`
						KeyPrefixEquals             string `xml:"KeyPrefixEquals"`
					} `xml:"Condition"`
					Redirect *redirect `xml:"Redirect"`
				} `xml:"RoutingRule"`
			} `xml:"RoutingRules"`
		}
		if xml.Unmarshal(raw, &configuration) != nil || configuration.XMLName.Local != "WebsiteConfiguration" {
			in["_body"] = string(raw)
			return
		}
		document := map[string]any{}
		convertRedirect := func(source *redirect) map[string]any {
			result := map[string]any{}
			if source.HostName != nil {
				result["HostName"] = *source.HostName
			}
			for key, value := range map[string]string{"HttpRedirectCode": source.HTTPRedirectCode, "Protocol": source.Protocol} {
				if value != "" {
					result[key] = value
				}
			}
			if source.ReplaceKeyPrefixWith != nil {
				result["ReplaceKeyPrefixWith"] = *source.ReplaceKeyPrefixWith
			}
			if source.ReplaceKeyWith != nil {
				result["ReplaceKeyWith"] = *source.ReplaceKeyWith
			}
			return result
		}
		if source := configuration.RedirectAllRequestsTo; source != nil {
			document["RedirectAllRequestsTo"] = convertRedirect(source)
		}
		if source := configuration.IndexDocument; source != nil {
			value := map[string]any{}
			if source.Suffix != nil {
				value["Suffix"] = *source.Suffix
			}
			document["IndexDocument"] = value
		}
		if source := configuration.ErrorDocument; source != nil {
			value := map[string]any{}
			if source.Key != nil {
				value["Key"] = *source.Key
			}
			document["ErrorDocument"] = value
		}
		if source := configuration.RoutingRules; source != nil {
			rules := make([]any, 0, len(source.Rules))
			for _, source := range source.Rules {
				rule := map[string]any{}
				if source.Condition != nil {
					condition := map[string]any{}
					for key, value := range map[string]string{"HttpErrorCodeReturnedEquals": source.Condition.HTTPErrorCodeReturnedEquals, "KeyPrefixEquals": source.Condition.KeyPrefixEquals} {
						if value != "" {
							condition[key] = value
						}
					}
					rule["Condition"] = condition
				}
				if source.Redirect != nil {
					rule["Redirect"] = convertRedirect(source.Redirect)
				}
				rules = append(rules, rule)
			}
			document["RoutingRules"] = rules
		}
		in["WebsiteConfiguration"] = document
	case "PutObjectLegalHold":
		var hold struct {
			Status string `xml:"Status"`
		}
		if xml.Unmarshal(raw, &hold) != nil {
			in["_body"] = string(raw)
			return
		}
		in["LegalHold"] = map[string]any{"Status": hold.Status}
	case "PutObjectRetention":
		var retention struct {
			Mode            string `xml:"Mode"`
			RetainUntilDate string `xml:"RetainUntilDate"`
		}
		if xml.Unmarshal(raw, &retention) != nil {
			in["_body"] = string(raw)
			return
		}
		in["Retention"] = map[string]any{"Mode": retention.Mode, "RetainUntilDate": retention.RetainUntilDate}
	case "PutBucketObjectLockConfiguration", "PutObjectLockConfiguration":
		var configuration struct {
			ObjectLockEnabled string `xml:"ObjectLockEnabled"`
			Rule              *struct {
				DefaultRetention *struct {
					Mode  string `xml:"Mode"`
					Days  *int   `xml:"Days"`
					Years *int   `xml:"Years"`
				} `xml:"DefaultRetention"`
			} `xml:"Rule"`
		}
		if xml.Unmarshal(raw, &configuration) != nil {
			in["_body"] = string(raw)
			return
		}
		document := map[string]any{"ObjectLockEnabled": configuration.ObjectLockEnabled}
		if configuration.Rule != nil && configuration.Rule.DefaultRetention != nil {
			retention := map[string]any{"Mode": configuration.Rule.DefaultRetention.Mode}
			if configuration.Rule.DefaultRetention.Days != nil {
				retention["Days"] = *configuration.Rule.DefaultRetention.Days
			}
			if configuration.Rule.DefaultRetention.Years != nil {
				retention["Years"] = *configuration.Rule.DefaultRetention.Years
			}
			document["Rule"] = map[string]any{"DefaultRetention": retention}
		}
		in["ObjectLockConfiguration"] = document
	case "PutBucketReplication":
		type tag struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		}
		type filter struct {
			Prefix string `xml:"Prefix"`
			Tag    *tag   `xml:"Tag"`
			And    *struct {
				Prefix string `xml:"Prefix"`
				Tags   []tag  `xml:"Tag"`
			} `xml:"And"`
		}
		var configuration struct {
			Role  string `xml:"Role"`
			Rules []struct {
				ID       string  `xml:"ID"`
				Priority *int    `xml:"Priority"`
				Status   string  `xml:"Status"`
				Prefix   string  `xml:"Prefix"`
				Filter   *filter `xml:"Filter"`
				Delete   *struct {
					Status string `xml:"Status"`
				} `xml:"DeleteMarkerReplication"`
				Destination struct {
					Bucket       string `xml:"Bucket"`
					Account      string `xml:"Account"`
					StorageClass string `xml:"StorageClass"`
				} `xml:"Destination"`
			} `xml:"Rule"`
		}
		if xml.Unmarshal(raw, &configuration) != nil {
			in["_body"] = string(raw)
			return
		}
		rules := make([]any, 0, len(configuration.Rules))
		for _, source := range configuration.Rules {
			rule := map[string]any{"Status": source.Status, "Destination": map[string]any{"Bucket": source.Destination.Bucket}}
			for key, value := range map[string]string{"ID": source.ID, "Prefix": source.Prefix} {
				if value != "" {
					rule[key] = value
				}
			}
			if source.Priority != nil {
				rule["Priority"] = *source.Priority
			}
			if source.Filter != nil {
				value := map[string]any{}
				if source.Filter.Prefix != "" {
					value["Prefix"] = source.Filter.Prefix
				}
				if source.Filter.Tag != nil {
					value["Tag"] = map[string]any{"Key": source.Filter.Tag.Key, "Value": source.Filter.Tag.Value}
				}
				if source.Filter.And != nil {
					and := map[string]any{"Prefix": source.Filter.And.Prefix}
					tags := make([]any, 0, len(source.Filter.And.Tags))
					for _, item := range source.Filter.And.Tags {
						tags = append(tags, map[string]any{"Key": item.Key, "Value": item.Value})
					}
					and["Tags"] = tags
					value["And"] = and
				}
				rule["Filter"] = value
			}
			if source.Delete != nil {
				rule["DeleteMarkerReplication"] = map[string]any{"Status": source.Delete.Status}
			}
			destination := rule["Destination"].(map[string]any)
			if source.Destination.Account != "" {
				destination["Account"] = source.Destination.Account
			}
			if source.Destination.StorageClass != "" {
				destination["StorageClass"] = source.Destination.StorageClass
			}
			rules = append(rules, rule)
		}
		in["ReplicationConfiguration"] = map[string]any{"Role": configuration.Role, "Rules": rules}
	case "PutBucketPolicy":
		in["Policy"] = string(raw)
	case "PutBucketLifecycleConfiguration":
		type tag struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		}
		type andFilter struct {
			Prefix                *string `xml:"Prefix"`
			Tags                  []tag   `xml:"Tag"`
			ObjectSizeGreaterThan *int64  `xml:"ObjectSizeGreaterThan"`
			ObjectSizeLessThan    *int64  `xml:"ObjectSizeLessThan"`
		}
		type filter struct {
			Prefix                *string    `xml:"Prefix"`
			Tag                   *tag       `xml:"Tag"`
			And                   *andFilter `xml:"And"`
			ObjectSizeGreaterThan *int64     `xml:"ObjectSizeGreaterThan"`
			ObjectSizeLessThan    *int64     `xml:"ObjectSizeLessThan"`
		}
		type expiration struct {
			Date                      *string `xml:"Date"`
			Days                      *int    `xml:"Days"`
			ExpiredObjectDeleteMarker *bool   `xml:"ExpiredObjectDeleteMarker"`
		}
		type transition struct {
			Date         *string `xml:"Date"`
			Days         *int    `xml:"Days"`
			StorageClass string  `xml:"StorageClass"`
		}
		type noncurrentTransition struct {
			NewerNoncurrentVersions *int   `xml:"NewerNoncurrentVersions"`
			NoncurrentDays          *int   `xml:"NoncurrentDays"`
			StorageClass            string `xml:"StorageClass"`
		}
		type noncurrentExpiration struct {
			NewerNoncurrentVersions *int `xml:"NewerNoncurrentVersions"`
			NoncurrentDays          *int `xml:"NoncurrentDays"`
		}
		var configuration struct {
			XMLName xml.Name
			Rules   []struct {
				ID                           *string                `xml:"ID"`
				Prefix                       *string                `xml:"Prefix"`
				Filter                       *filter                `xml:"Filter"`
				Status                       *string                `xml:"Status"`
				Expiration                   *expiration            `xml:"Expiration"`
				Transitions                  []transition           `xml:"Transition"`
				NoncurrentVersionTransitions []noncurrentTransition `xml:"NoncurrentVersionTransition"`
				NoncurrentVersionExpiration  *noncurrentExpiration  `xml:"NoncurrentVersionExpiration"`
				AbortIncompleteMultipart     *struct {
					DaysAfterInitiation *int `xml:"DaysAfterInitiation"`
				} `xml:"AbortIncompleteMultipartUpload"`
			} `xml:"Rule"`
		}
		if xml.Unmarshal(raw, &configuration) != nil || configuration.XMLName.Local != "LifecycleConfiguration" {
			in["_body"] = string(raw)
			return
		}
		addInt := func(target map[string]any, key string, value *int) {
			if value != nil {
				target[key] = *value
			}
		}
		addInt64 := func(target map[string]any, key string, value *int64) {
			if value != nil {
				target[key] = *value
			}
		}
		addString := func(target map[string]any, key string, value *string) {
			if value != nil {
				target[key] = *value
			}
		}
		convertTags := func(source []tag) []any {
			values := make([]any, 0, len(source))
			for _, item := range source {
				values = append(values, map[string]any{"Key": item.Key, "Value": item.Value})
			}
			return values
		}
		rules := make([]any, 0, len(configuration.Rules))
		for _, source := range configuration.Rules {
			rule := map[string]any{}
			addString(rule, "ID", source.ID)
			addString(rule, "Prefix", source.Prefix)
			addString(rule, "Status", source.Status)
			if source.Filter != nil {
				value := map[string]any{}
				addString(value, "Prefix", source.Filter.Prefix)
				addInt64(value, "ObjectSizeGreaterThan", source.Filter.ObjectSizeGreaterThan)
				addInt64(value, "ObjectSizeLessThan", source.Filter.ObjectSizeLessThan)
				if source.Filter.Tag != nil {
					value["Tag"] = map[string]any{"Key": source.Filter.Tag.Key, "Value": source.Filter.Tag.Value}
				}
				if source.Filter.And != nil {
					and := map[string]any{}
					addString(and, "Prefix", source.Filter.And.Prefix)
					addInt64(and, "ObjectSizeGreaterThan", source.Filter.And.ObjectSizeGreaterThan)
					addInt64(and, "ObjectSizeLessThan", source.Filter.And.ObjectSizeLessThan)
					if len(source.Filter.And.Tags) != 0 {
						and["Tags"] = convertTags(source.Filter.And.Tags)
					}
					value["And"] = and
				}
				rule["Filter"] = value
			}
			if source.Expiration != nil {
				value := map[string]any{}
				addString(value, "Date", source.Expiration.Date)
				addInt(value, "Days", source.Expiration.Days)
				if source.Expiration.ExpiredObjectDeleteMarker != nil {
					value["ExpiredObjectDeleteMarker"] = *source.Expiration.ExpiredObjectDeleteMarker
				}
				rule["Expiration"] = value
			}
			for field, values := range map[string]any{
				"Transitions": source.Transitions, "NoncurrentVersionTransitions": source.NoncurrentVersionTransitions,
			} {
				items := []any{}
				switch typed := values.(type) {
				case []transition:
					for _, item := range typed {
						value := map[string]any{"StorageClass": item.StorageClass}
						addString(value, "Date", item.Date)
						addInt(value, "Days", item.Days)
						items = append(items, value)
					}
				case []noncurrentTransition:
					for _, item := range typed {
						value := map[string]any{"StorageClass": item.StorageClass}
						addInt(value, "NewerNoncurrentVersions", item.NewerNoncurrentVersions)
						addInt(value, "NoncurrentDays", item.NoncurrentDays)
						items = append(items, value)
					}
				}
				if len(items) != 0 {
					rule[field] = items
				}
			}
			if source.NoncurrentVersionExpiration != nil {
				value := map[string]any{}
				addInt(value, "NewerNoncurrentVersions", source.NoncurrentVersionExpiration.NewerNoncurrentVersions)
				addInt(value, "NoncurrentDays", source.NoncurrentVersionExpiration.NoncurrentDays)
				rule["NoncurrentVersionExpiration"] = value
			}
			if source.AbortIncompleteMultipart != nil {
				value := map[string]any{}
				addInt(value, "DaysAfterInitiation", source.AbortIncompleteMultipart.DaysAfterInitiation)
				rule["AbortIncompleteMultipartUpload"] = value
			}
			rules = append(rules, rule)
		}
		in["LifecycleConfiguration"] = map[string]any{"Rules": rules}
	case "PutBucketEncryption":
		var configuration struct {
			Rules []struct {
				Defaults *struct {
					Algorithm string  `xml:"SSEAlgorithm"`
					KeyID     *string `xml:"KMSMasterKeyID"`
				} `xml:"ApplyServerSideEncryptionByDefault"`
				BucketKeyEnabled *bool `xml:"BucketKeyEnabled"`
			} `xml:"Rule"`
		}
		if xml.Unmarshal(raw, &configuration) != nil {
			in["_body"] = string(raw)
			return
		}
		rules := make([]any, 0, len(configuration.Rules))
		for _, source := range configuration.Rules {
			rule := map[string]any{}
			if source.Defaults != nil {
				defaults := map[string]any{"SSEAlgorithm": source.Defaults.Algorithm}
				if source.Defaults.KeyID != nil {
					defaults["KMSMasterKeyID"] = *source.Defaults.KeyID
				}
				rule["ApplyServerSideEncryptionByDefault"] = defaults
			}
			if source.BucketKeyEnabled != nil {
				rule["BucketKeyEnabled"] = *source.BucketKeyEnabled
			}
			rules = append(rules, rule)
		}
		in["ServerSideEncryptionConfiguration"] = map[string]any{"Rules": rules}
	case "PutBucketVersioning":
		var v struct {
			Status string `xml:"Status"`
		}
		if xml.Unmarshal(raw, &v) == nil && v.Status != "" {
			in["Status"] = v.Status
		} else {
			in["_body"] = string(raw)
		}
	case "PutBucketNotificationConfiguration":
		type notification struct {
			ID            string   `xml:"Id"`
			Queue         string   `xml:"Queue"`
			Topic         string   `xml:"Topic"`
			CloudFunction string   `xml:"CloudFunction"`
			Events        []string `xml:"Event"`
			Filter        *struct {
				Key *struct {
					Rules []struct {
						Name  string `xml:"Name"`
						Value string `xml:"Value"`
					} `xml:"FilterRule"`
				} `xml:"S3Key"`
			} `xml:"Filter"`
		}
		var n struct {
			XMLName                  xml.Name
			QueueConfigurations      []notification `xml:"QueueConfiguration"`
			TopicConfigurations      []notification `xml:"TopicConfiguration"`
			LambdaConfigurations     []notification `xml:"CloudFunctionConfiguration"`
			EventBridgeConfiguration *struct{}      `xml:"EventBridgeConfiguration"`
		}
		if xml.Unmarshal(raw, &n) != nil || n.XMLName.Local != "NotificationConfiguration" {
			in["_body"] = string(raw)
			break
		}
		document := map[string]any{}
		add := func(field, arnField string, sources []notification, arn func(notification) string) {
			if len(sources) == 0 {
				return
			}
			configurations := make([]any, 0, len(sources))
			for _, source := range sources {
				configuration := map[string]any{arnField: arn(source), "Events": stringsToAny(source.Events)}
				if source.ID != "" {
					configuration["Id"] = source.ID
				}
				if source.Filter != nil {
					filter := map[string]any{}
					if source.Filter.Key != nil {
						rules := make([]any, 0, len(source.Filter.Key.Rules))
						for _, rule := range source.Filter.Key.Rules {
							rules = append(rules, map[string]any{"Name": rule.Name, "Value": rule.Value})
						}
						filter["Key"] = map[string]any{"FilterRules": rules}
					}
					configuration["Filter"] = filter
				}
				configurations = append(configurations, configuration)
			}
			document[field] = configurations
		}
		add("QueueConfigurations", "QueueArn", n.QueueConfigurations, func(source notification) string { return source.Queue })
		add("TopicConfigurations", "TopicArn", n.TopicConfigurations, func(source notification) string { return source.Topic })
		add("LambdaFunctionConfigurations", "LambdaFunctionArn", n.LambdaConfigurations, func(source notification) string { return source.CloudFunction })
		if n.EventBridgeConfiguration != nil {
			document["EventBridgeConfiguration"] = map[string]any{}
		}
		in["NotificationConfiguration"] = document
	default:
		in["_body"] = string(raw)
	}
}

type namedXMLNode struct {
	XMLName  xml.Name
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
		status = 200
	}
	for k, vs := range resp.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if resp.Stream != nil {
		w.WriteHeader(status)
		_, err := io.Copy(w, resp.Stream)
		_ = resp.Stream.Close()
		return err
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
	if op.Name == "GetBucketEncryption" {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		if len(resp.Output) == 0 {
			return nil
		}
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, value := range resp.Output["Rules"].([]any) {
			rule, _ := value.(map[string]any)
			b.WriteString("<Rule>")
			if defaults, ok := rule["ApplyServerSideEncryptionByDefault"].(map[string]any); ok {
				b.WriteString("<ApplyServerSideEncryptionByDefault>")
				fmt.Fprintf(&b, "<SSEAlgorithm>%s</SSEAlgorithm>", xmlEscape(fmt.Sprint(defaults["SSEAlgorithm"])))
				if keyID, exists := defaults["KMSMasterKeyID"]; exists {
					fmt.Fprintf(&b, "<KMSMasterKeyID>%s</KMSMasterKeyID>", xmlEscape(fmt.Sprint(keyID)))
				}
				b.WriteString("</ApplyServerSideEncryptionByDefault>")
			}
			if enabled, exists := rule["BucketKeyEnabled"]; exists {
				fmt.Fprintf(&b, "<BucketKeyEnabled>%v</BucketKeyEnabled>", enabled)
			}
			b.WriteString("</Rule>")
		}
		b.WriteString("</ServerSideEncryptionConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if resp.Output == nil {
		return nil
	}
	type kv struct {
		XMLName xml.Name
		Value   string `xml:",chardata"`
	}
	// Simple XML object encoder.
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	if op.Name == "GetBucketLocation" {
		b.WriteString(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		_ = xml.EscapeText(&b, []byte(fmt.Sprint(resp.Output["LocationConstraint"])))
		b.WriteString(`</LocationConstraint>`)
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketRequestPayment" {
		b.WriteString(`<RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		write(resp.Output, &b)
		b.WriteString("</RequestPaymentConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketAccelerateConfiguration" {
		b.WriteString(`<AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		write(resp.Output, &b)
		b.WriteString("</AccelerateConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketLogging" {
		b.WriteString(`<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		write(resp.Output, &b)
		b.WriteString("</BucketLoggingStatus>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketCors" {
		b.WriteString(`<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, value := range resp.Output["CORSRules"].([]any) {
			rule, _ := value.(map[string]any)
			b.WriteString("<CORSRule>")
			for _, field := range []string{"ID", "AllowedHeaders", "AllowedMethods", "AllowedOrigins", "ExposeHeaders", "MaxAgeSeconds"} {
				value, ok := rule[field]
				if !ok {
					continue
				}
				tag := strings.TrimSuffix(field, "s")
				if values, ok := value.([]any); ok {
					for _, value := range values {
						fmt.Fprintf(&b, "<%s>%s</%s>", tag, xmlEscape(fmt.Sprint(value)), tag)
					}
					continue
				}
				fmt.Fprintf(&b, "<%s>%s</%s>", field, xmlEscape(fmt.Sprint(value)), field)
			}
			b.WriteString("</CORSRule>")
		}
		b.WriteString("</CORSConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketWebsite" {
		b.WriteString(`<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, field := range []string{"RedirectAllRequestsTo", "IndexDocument", "ErrorDocument"} {
			if value, ok := resp.Output[field]; ok {
				fmt.Fprintf(&b, "<%s>", field)
				write(value, &b)
				fmt.Fprintf(&b, "</%s>", field)
			}
		}
		if rules, ok := resp.Output["RoutingRules"].([]any); ok {
			b.WriteString("<RoutingRules>")
			for _, value := range rules {
				rule, _ := value.(map[string]any)
				b.WriteString("<RoutingRule>")
				for _, field := range []string{"Condition", "Redirect"} {
					if value, ok := rule[field]; ok {
						fmt.Fprintf(&b, "<%s>", field)
						write(value, &b)
						fmt.Fprintf(&b, "</%s>", field)
					}
				}
				b.WriteString("</RoutingRule>")
			}
			b.WriteString("</RoutingRules>")
		}
		b.WriteString("</WebsiteConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketLifecycleConfiguration" {
		b.WriteString(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		writeLifecycle(resp.Output, &b)
		b.WriteString("</LifecycleConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketNotificationConfiguration" {
		b.WriteString(`<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, destination := range []struct{ field, tag, arnField, arnTag string }{
			{"TopicConfigurations", "TopicConfiguration", "TopicArn", "Topic"},
			{"QueueConfigurations", "QueueConfiguration", "QueueArn", "Queue"},
			{"LambdaFunctionConfigurations", "CloudFunctionConfiguration", "LambdaFunctionArn", "CloudFunction"},
		} {
			configurations, _ := resp.Output[destination.field].([]any)
			for _, value := range configurations {
				configuration, _ := value.(map[string]any)
				fmt.Fprintf(&b, "<%s>", destination.tag)
				id, _ := configuration["Id"].(string)
				if id != "" {
					fmt.Fprintf(&b, "<Id>%s</Id>", xmlEscape(id))
				}
				arn, _ := configuration[destination.arnField].(string)
				fmt.Fprintf(&b, "<%s>%s</%s>", destination.arnTag, xmlEscape(arn), destination.arnTag)
				events, _ := configuration["Events"].([]any)
				for _, event := range events {
					fmt.Fprintf(&b, "<Event>%s</Event>", xmlEscape(fmt.Sprint(event)))
				}
				if filter, ok := configuration["Filter"].(map[string]any); ok {
					b.WriteString("<Filter><S3Key>")
					key, _ := filter["Key"].(map[string]any)
					rules, _ := key["FilterRules"].([]any)
					for _, value := range rules {
						b.WriteString("<FilterRule>")
						write(value, &b)
						b.WriteString("</FilterRule>")
					}
					b.WriteString("</S3Key></Filter>")
				}
				fmt.Fprintf(&b, "</%s>", destination.tag)
			}
		}
		if _, ok := resp.Output["EventBridgeConfiguration"]; ok {
			b.WriteString("<EventBridgeConfiguration></EventBridgeConfiguration>")
		}
		b.WriteString("</NotificationConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketAcl" || op.Name == "GetObjectAcl" {
		b.WriteString(`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		owner, _ := resp.Output["Owner"].(map[string]any)
		b.WriteString("<Owner>")
		for _, field := range []string{"ID", "DisplayName"} {
			if value := xmlString(owner[field]); value != "" {
				fmt.Fprintf(&b, "<%s>%s</%s>", field, xmlEscape(value), field)
			}
		}
		b.WriteString("</Owner><AccessControlList>")
		for _, value := range asAnySlice(resp.Output["Grants"]) {
			grant, _ := value.(map[string]any)
			grantee, _ := grant["Grantee"].(map[string]any)
			b.WriteString(`<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="` + xmlEscape(xmlString(grantee["Type"])) + `">`)
			for _, field := range []string{"ID", "DisplayName", "URI", "EmailAddress"} {
				if value := xmlString(grantee[field]); value != "" {
					fmt.Fprintf(&b, "<%s>%s</%s>", field, xmlEscape(value), field)
				}
			}
			b.WriteString("</Grantee><Permission>" + xmlEscape(xmlString(grant["Permission"])) + "</Permission></Grant>")
		}
		b.WriteString("</AccessControlList></AccessControlPolicy>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if shape := namedConfigurationShape(op.Name); shape.configuration != "" {
		if strings.HasPrefix(op.Name, "Get") {
			fmt.Fprintf(&b, `<%s xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`, shape.configuration)
			writeNamedConfiguration(resp.Output[shape.configuration], &b)
			fmt.Fprintf(&b, "</%s>", shape.configuration)
			_, err := io.WriteString(w, b.String())
			return err
		}
		if strings.HasPrefix(op.Name, "List") {
			root := op.Name + "Result"
			fmt.Fprintf(&b, `<%s xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`, root)
			for _, field := range []string{"ContinuationToken", "NextContinuationToken", "IsTruncated"} {
				if value, ok := resp.Output[field]; ok {
					fmt.Fprintf(&b, "<%s>", field)
					writeNamedConfiguration(value, &b)
					fmt.Fprintf(&b, "</%s>", field)
				}
			}
			for _, value := range asAnySlice(resp.Output[shape.list]) {
				fmt.Fprintf(&b, "<%s>", shape.configuration)
				writeNamedConfiguration(value, &b)
				fmt.Fprintf(&b, "</%s>", shape.configuration)
			}
			fmt.Fprintf(&b, "</%s>", root)
			_, err := io.WriteString(w, b.String())
			return err
		}
	}
	configurationRoot := map[string]string{
		"GetBucketObjectLockConfiguration": "ObjectLockConfiguration",
		"GetObjectLockConfiguration":       "ObjectLockConfiguration",
		"GetObjectLegalHold":               "LegalHold",
		"GetObjectRetention":               "Retention",
		"GetPublicAccessBlock":             "PublicAccessBlockConfiguration",
	}[op.Name]
	if configurationRoot != "" {
		fmt.Fprintf(&b, `<%s xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`, configurationRoot)
		write(resp.Output[configurationRoot], &b)
		fmt.Fprintf(&b, "</%s>", configurationRoot)
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketOwnershipControls" {
		b.WriteString(`<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		controls, _ := resp.Output["OwnershipControls"].(map[string]any)
		rules, _ := controls["Rules"].([]any)
		for _, rule := range rules {
			b.WriteString("<Rule>")
			write(rule, &b)
			b.WriteString("</Rule>")
		}
		b.WriteString("</OwnershipControls>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	if op.Name == "GetBucketReplication" {
		configuration, _ := resp.Output["ReplicationConfiguration"].(map[string]any)
		if configuration == nil {
			configuration = resp.Output
		}
		b.WriteString(`<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if role := configuration["Role"]; role != nil {
			write(map[string]any{"Role": role}, &b)
		}
		rules, _ := configuration["Rules"].([]any)
		for _, rule := range rules {
			b.WriteString("<Rule>")
			document, _ := rule.(map[string]any)
			fields := make(map[string]any, len(document))
			for key, value := range document {
				if key != "Filter" {
					fields[key] = value
				}
			}
			write(fields, &b)
			if filter, ok := document["Filter"].(map[string]any); ok {
				b.WriteString("<Filter>")
				filterFields := make(map[string]any, len(filter))
				for key, value := range filter {
					if key != "And" {
						filterFields[key] = value
					}
				}
				write(filterFields, &b)
				if and, ok := filter["And"].(map[string]any); ok {
					b.WriteString("<And>")
					andFields := make(map[string]any, len(and))
					for key, value := range and {
						if key != "Tags" {
							andFields[key] = value
						}
					}
					write(andFields, &b)
					tags, _ := and["Tags"].([]any)
					for _, tag := range tags {
						b.WriteString("<Tag>")
						write(tag, &b)
						b.WriteString("</Tag>")
					}
					b.WriteString("</And>")
				}
				b.WriteString("</Filter>")
			}
			b.WriteString("</Rule>")
		}
		b.WriteString("</ReplicationConfiguration>")
		_, err := io.WriteString(w, b.String())
		return err
	}
	root := op.Name + "Result"
	if op.Name == "ListBuckets" {
		root = "ListAllMyBucketsResult"
	}
	if op.Name == "ListObjectsV2" || op.Name == "ListObjects" {
		root = "ListBucketResult"
	}
	if op.Name == "GetObjectAttributes" {
		root = "GetObjectAttributesResponse"
	}
	if op.Name == "PostObject" {
		root = "PostResponse"
	}
	namespace := ""
	if op.Name == "DeleteObjects" {
		root = "DeleteResult"
		namespace = ` xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`
	}
	fmt.Fprintf(&b, "<%s%s>", root, namespace)
	switch op.Name {
	case "ListBuckets":
		top := make(map[string]any, len(resp.Output)-1)
		for key, value := range resp.Output {
			if key != "Buckets" {
				top[key] = value
			}
		}
		write(top, &b)
		b.WriteString("<Buckets>")
		buckets, _ := resp.Output["Buckets"].([]any)
		for _, item := range buckets {
			b.WriteString("<Bucket>")
			write(item, &b)
			b.WriteString("</Bucket>")
		}
		b.WriteString("</Buckets>")
	case "ListParts":
		writeFlattened(resp.Output, &b, [][2]string{{"Parts", "Part"}})
	case "ListMultipartUploads":
		writeFlattened(resp.Output, &b, [][2]string{{"Uploads", "Upload"}, {"CommonPrefixes", "CommonPrefixes"}})
	case "GetObjectAttributes":
		top := make(map[string]any, len(resp.Output)-1)
		for key, value := range resp.Output {
			if key != "ObjectParts" {
				top[key] = value
			}
		}
		write(top, &b)
		if parts, ok := resp.Output["ObjectParts"].(map[string]any); ok {
			b.WriteString("<ObjectParts>")
			encoded := make(map[string]any, len(parts))
			for key, value := range parts {
				encoded[key] = value
			}
			if count, ok := encoded["TotalPartsCount"]; ok {
				delete(encoded, "TotalPartsCount")
				encoded["PartsCount"] = count
			}
			writeFlattened(encoded, &b, [][2]string{{"Parts", "Part"}})
			b.WriteString("</ObjectParts>")
		}
	case "GetObjectTagging", "GetBucketTagging":
		b.WriteString("<TagSet>")
		for _, item := range resp.Output["TagSet"].([]any) {
			b.WriteString("<Tag>")
			write(item, &b)
			b.WriteString("</Tag>")
		}
		b.WriteString("</TagSet>")
	case "DeleteObjects":
		writeFlattened(resp.Output, &b, [][2]string{{"Deleted", "Deleted"}, {"Errors", "Error"}})
	default:
		write(resp.Output, &b)
	}
	fmt.Fprintf(&b, "</%s>", root)
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

func asAnySlice(value any) []any {
	result, _ := value.([]any)
	return result
}

func writeNamedConfiguration(value any, b *strings.Builder) {
	switch value := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			items, isSlice := value[key].([]any)
			if key == "OptionalFields" && isSlice {
				b.WriteString("<OptionalFields>")
				for _, item := range items {
					b.WriteString("<Field>")
					writeNamedConfiguration(item, b)
					b.WriteString("</Field>")
				}
				b.WriteString("</OptionalFields>")
				continue
			}
			if (key == "Tags" || key == "Tierings") && isSlice {
				tag := strings.TrimSuffix(key, "s")
				for _, item := range items {
					fmt.Fprintf(b, "<%s>", tag)
					writeNamedConfiguration(item, b)
					fmt.Fprintf(b, "</%s>", tag)
				}
				continue
			}
			fmt.Fprintf(b, "<%s>", key)
			writeNamedConfiguration(value[key], b)
			fmt.Fprintf(b, "</%s>", key)
		}
	case nil:
	default:
		b.WriteString(xmlEscape(fmt.Sprint(value)))
	}
}

func writeFlattened(output map[string]any, b *strings.Builder, members [][2]string) {
	top := make(map[string]any, len(output)-len(members))
	for key, value := range output {
		top[key] = value
	}
	for _, member := range members {
		delete(top, member[0])
	}
	write(top, b)
	for _, member := range members {
		items, _ := output[member[0]].([]any)
		for _, item := range items {
			fmt.Fprintf(b, "<%s>", member[1])
			write(item, b)
			fmt.Fprintf(b, "</%s>", member[1])
		}
	}
}

func stringsToAny(values []string) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

func write(v any, b *strings.Builder) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, "<%s>", k)
			write(t[k], b)
			fmt.Fprintf(b, "</%s>", k)
		}
	case []any:
		for _, item := range t {
			b.WriteString("<member>")
			write(item, b)
			b.WriteString("</member>")
		}
	case nil:
	default:
		b.WriteString(xmlEscape(fmt.Sprint(t)))
	}
}

func writeLifecycle(v any, b *strings.Builder) {
	switch value := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			tag := map[string]string{
				"Rules": "Rule", "Tags": "Tag", "Transitions": "Transition",
				"NoncurrentVersionTransitions": "NoncurrentVersionTransition",
			}[key]
			if tag == "" {
				tag = key
			}
			if items, ok := value[key].([]any); ok {
				for _, item := range items {
					fmt.Fprintf(b, "<%s>", tag)
					writeLifecycle(item, b)
					fmt.Fprintf(b, "</%s>", tag)
				}
				continue
			}
			fmt.Fprintf(b, "<%s>", tag)
			writeLifecycle(value[key], b)
			fmt.Fprintf(b, "</%s>", tag)
		}
	case nil:
	default:
		b.WriteString(xmlEscape(fmt.Sprint(value)))
	}
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
	if region := xmlString(f.Fields["Region"]); region != "" {
		w.Header().Set("x-amz-bucket-region", region)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	var body strings.Builder
	fmt.Fprintf(&body, `<Error><Code>%s</Code><Message>%s</Message>`, xmlEscape(f.Code), xmlEscape(f.Message))
	write(f.Fields, &body)
	fmt.Fprintf(&body, `<RequestId>%s</RequestId><HostId>mirror</HostId></Error>`, xmlEscape(requestID))
	_, err := io.WriteString(w, body.String())
	return err
}

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}

func xmlString(value any) string {
	result, _ := value.(string)
	return result
}
