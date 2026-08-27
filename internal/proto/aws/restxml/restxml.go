// Package restxml implements the S3 restXml codec.
package restxml

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
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
	if src := r.Header.Get("x-amz-copy-source"); src != "" {
		in["CopySource"] = strings.TrimPrefix(src, "/")
	}
	req := &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}
	streamOps := op.Name == "PutObject" || op.Name == "UploadPart"
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
	return req, nil
}

func parseXMLInput(op string, raw []byte, in map[string]any) {
	switch op {
	case "CreateBucket":
		var cfg struct {
			LocationConstraint string `xml:"LocationConstraint"`
		}
		if xml.Unmarshal(raw, &cfg) != nil {
			in["_body"] = string(raw)
			return
		}
		in["LocationConstraint"] = cfg.LocationConstraint
		in["CreateBucketConfiguration"] = map[string]any{"LocationConstraint": cfg.LocationConstraint}
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
	case "PutBucketPolicy":
		in["Policy"] = string(raw)
	case "PutBucketCors", "PutBucketWebsite", "PutBucketLogging",
		"PutBucketLifecycleConfiguration", "PutBucketReplication",
		"PutBucketEncryption", "PutBucketAcl", "PutObjectAcl",
		"PutBucketObjectLockConfiguration", "PutBucketRequestPayment",
		"PutBucketAccelerateConfiguration":
		in["_body"] = string(raw)
		in["Document"] = string(raw)
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
		var n struct {
			QueueConfiguration []struct {
				Queue string   `xml:"Queue"`
				Event []string `xml:"Event"`
			} `xml:"QueueConfiguration"`
			TopicConfiguration []struct {
				Topic string   `xml:"Topic"`
				Event []string `xml:"Event"`
			} `xml:"TopicConfiguration"`
		}
		if xml.Unmarshal(raw, &n) != nil {
			in["_body"] = string(raw)
			break
		}
		var qs, ts []any
		for _, q := range n.QueueConfiguration {
			qs = append(qs, map[string]any{"QueueArn": q.Queue, "Queue": q.Queue, "Events": q.Event})
		}
		for _, t := range n.TopicConfiguration {
			ts = append(ts, map[string]any{"TopicArn": t.Topic, "Events": t.Event})
		}
		in["QueueConfigurations"] = qs
		in["TopicConfigurations"] = ts
	default:
		in["_body"] = string(raw)
	}
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

func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		status = 400
	}
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, err := fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message><RequestId>%s</RequestId><HostId>mirror</HostId></Error>`, xmlEscape(f.Code), xmlEscape(f.Message), xmlEscape(requestID))
	return err
}

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}
