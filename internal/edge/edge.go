// Package edge is the HTTP demux: identity, codecs, packs, diagnostics.
package edge

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/identity"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/idgen"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/logging"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/mock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/awsjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/awsquery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/restjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/restxml"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/gcp/gcprest"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Server is the HTTP front door.
type Server struct {
	cfg       config.Config
	deps      spi.Deps
	reg       registry.Registry
	bundle    *model.Bundle
	codecs    map[model.Protocol]proto.Codec
	started   time.Time
	mocks     map[string]*mock.Pack
	version   string
	advertise string
}

// New constructs a server.
func New(cfg config.Config, deps spi.Deps, reg registry.Registry, version string) *Server {
	b := deps.Model
	if b == nil || len(b.Services) == 0 {
		b = catalog.Bundle()
		deps.Model = b
	}
	s := &Server{
		cfg:     cfg,
		deps:    deps,
		reg:     reg,
		bundle:  b,
		started: deps.Clock.Now(),
		version: version,
		mocks:   map[string]*mock.Pack{},
		codecs: map[model.Protocol]proto.Codec{
			model.ProtoAWSJSON10:  awsjson.New10(),
			model.ProtoAWSJSON11:  awsjson.New11(),
			model.ProtoRESTJSON1:  restjson.Codec{},
			model.ProtoRESTXML:    restxml.Codec{},
			model.ProtoAWSQuery:   awsquery.Codec{},
			model.ProtoEC2Query:   awsquery.Codec{},
			model.ProtoGCPRESTSON: gcprest.Codec{},
		},
		advertise: cfg.AdvertiseURL,
	}
	for i := range b.Services {
		svc := &b.Services[i]
		s.mocks[svc.ID] = mock.New(svc, deps, cfg.Strict)
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var awsChunks [][]byte
	var awsChunkSignatures []string
	var awsTrailers http.Header
	var awsDecodedLength int64
	awsChunkedDecoded := false
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.WriteHeader(204)
		return
	}
	if r.Header.Get("Expect") == "100-continue" {
		w.WriteHeader(http.StatusContinue)
	}
	if strings.HasPrefix(r.URL.Path, "/_mirror/") {
		s.diag(w, r)
		return
	}
	start := s.deps.Clock.Now()
	rid := idgen.Next(s.deps.Rand)
	ctx := logging.WithRequestID(r.Context(), rid)
	r = r.WithContext(ctx)

	if enc := r.Header.Get("Content-Encoding"); strings.Contains(strings.ToLower(enc), "aws-chunked") || r.Header.Get("X-Amz-Decoded-Content-Length") != "" {
		raw, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err == nil {
			body := raw
			if deframed, chunks, signatures, trailers, err2 := parseAWSChunked(bytes.NewReader(raw)); err2 == nil {
				body = deframed
				awsChunks = chunks
				awsChunkSignatures = signatures
				awsTrailers = trailers
				awsDecodedLength = int64(len(body))
				awsChunkedDecoded = true
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
	}

	svc := s.demux(r)
	w.Header().Set("x-mirror-request-id", rid)
	if svc != nil && svc.ID == "aws.s3" {
		w.Header().Set("x-amz-request-id", rid)
		w.Header().Set("x-amz-id-2", "mirror-"+rid)
		if fault := identity.PresignedAuthFault(r); fault != nil {
			s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "unknown"}, fault, rid)
			return
		}
	}
	id := identity.Parse(r, s.cfg.DefaultAccount, s.cfg.DefaultRegion, s.deps.Clock.Now())
	if identity.Expired(id) {
		if svc != nil && svc.ID == "aws.s3" {
			fields := map[string]any{"ServerTime": s.deps.Clock.Now().UTC().Format(time.RFC3339)}
			if expires, ok := identity.PresignedExpiry(r); ok {
				fields["Expires"] = expires.UTC().Format(time.RFC3339)
			}
			if value := r.URL.Query().Get("X-Amz-Expires"); value != "" {
				fields["X-Amz-Expires"] = value
			}
			s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "unknown"}, &spi.Fault{Code: "AccessDenied", Message: "Request has expired", HTTPStatus: http.StatusForbidden, Fault: "client", Fields: fields}, rid)
			return
		}
		http.Error(w, "Request has expired", http.StatusForbidden)
		return
	}
	if svc != nil && svc.ID == "aws.s3" && s.cfg.S3ValidatePresignedSignatures {
		secret := "test"
		if id.AccessKeyID != "test" {
			secret = s.deps.Rand.Derive(id.AccessKeyID).Hex(40)
		}
		if _, temporary, _ := s.deps.Store.Scope("_mirror", "global").Collection("stsk").Get(ctx, id.AccessKeyID); temporary {
			if fault := identity.VerifyS3SessionToken(r, s.deps.Rand.Derive(id.AccessKeyID+"tok").Hex(32)); fault != nil {
				s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "unknown"}, fault, rid)
				return
			}
		}
		fault := identity.VerifyS3Signature(r, id.AccessKeyID, secret, id.Region)
		if host := signedGatewayHost(r.Host, s.cfg.Bind); fault != nil && host != "" {
			candidate := r.Clone(ctx)
			candidate.Host = host
			fault = identity.VerifyS3Signature(candidate, id.AccessKeyID, secret, id.Region)
		}
		if fault == nil {
			fault = identity.S3AuthorizationTimeFault(r, s.deps.Clock.Now())
		}
		if fault == nil {
			fault = identity.VerifyS3StreamingV4(r, secret, awsChunks, awsChunkSignatures, awsTrailers)
		}
		if fault != nil {
			s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "unknown"}, fault, rid)
			return
		}
	}
	if awsChunkedDecoded {
		r.ContentLength = awsDecodedLength
		for name, values := range awsTrailers {
			if strings.EqualFold(name, "X-Amz-Trailer-Signature") {
				continue
			}
			r.Header[name] = append([]string(nil), values...)
		}
	}

	if svc == nil {
		w.Header().Set("x-mirror-fidelity", "emulate")
		http.Error(w, "MirrorNotImplemented: unknown service", http.StatusNotImplemented)
		return
	}
	codec := s.codecs[svc.Protocol]
	if codec == nil {
		http.Error(w, "no codec", 500)
		return
	}

	// SQS dual protocol: json if X-Amz-Target, else query.
	if svc.ID == "aws.sqs" {
		if r.Header.Get("X-Amz-Target") != "" {
			codec = s.codecs[model.ProtoAWSJSON10]
		} else {
			codec = s.codecs[model.ProtoAWSQuery]
		}
	}

	op, err := codec.Route(svc, r)
	if err != nil {
		s.fault(w, codec, svc, &model.Operation{Name: "unknown"}, err, rid)
		return
	}
	req, err := codec.Decode(svc, op, r)
	if err != nil {
		s.fault(w, codec, svc, op, err, rid)
		return
	}
	req.Identity = id
	req.HTTP = r
	req.S3ValidateSignatures = svc.ID == "aws.s3" && s.cfg.S3ValidatePresignedSignatures

	if !s.serviceEnabled(svc.ID) {
		s.fault(w, codec, svc, op, spi.NotImplemented(svc.ID, op.Name, "emulate"), rid)
		return
	}
	pack, ok := s.reg.Resolve(svc.ID)
	tier := model.TierMock
	if !ok || pack == nil || !contains(pack.Operations(), op.Name) {
		pack = s.mocks[svc.ID]
		tier = model.TierMock
	} else {
		tier = pack.Tier()
	}
	if pack == nil {
		s.fault(w, codec, svc, op, spi.NotImplemented(svc.ID, op.Name, "emulate"), rid)
		return
	}

	if s.deps.Authorizer != nil && svc.ID != "aws.sts" && svc.ID != "aws.iam" {
		for _, check := range authorizationChecks(req) {
			var authErr error
			if authorizer, ok := s.deps.Authorizer.(spi.RequestAuthorizer); ok {
				child := *req
				child.Operation = check.operation
				authErr = authorizer.AuthorizeRequest(ctx, &child, check.resource)
			} else {
				authErr = s.deps.Authorizer.Authorize(ctx, id, svc.ID, check.operation, check.resource)
			}
			if authErr != nil {
				s.fault(w, codec, svc, op, authErr, rid)
				return
			}
		}
	}

	resp, err := pack.Invoke(ctx, req)
	dur := s.deps.Clock.Since(start)
	code := 200
	errCode := ""
	if err != nil {
		if f, ok := err.(*spi.Fault); ok {
			code = f.HTTPStatus
			errCode = f.Code
		} else {
			code = 500
			errCode = "InternalError"
		}
	} else if resp != nil && resp.Status != 0 {
		code = resp.Status
	}
	s.deps.Journal.Record(spi.Entry{
		At: start, RequestID: rid, ServiceID: svc.ID, Operation: op.Name,
		Tier: tier, Account: id.Account, Region: id.Region, Status: code,
		ErrorCode: errCode, Duration: dur,
	})
	w.Header().Set("x-mirror-fidelity", string(tier))
	if err != nil {
		s.fault(w, codec, svc, op, err, rid)
		return
	}
	if resp.Headers == nil {
		resp.Headers = http.Header{}
	}
	resp.Headers.Set("x-mirror-fidelity", string(tier))
	resp.Headers.Set("x-mirror-request-id", rid)
	_ = codec.Encode(svc, op, w, resp)
}

func signedGatewayHost(host, bind string) string {
	_, port, err := net.SplitHostPort(bind)
	if err != nil || port == "" || port == "0" {
		return ""
	}
	hostname := host
	if parsedHost, parsedPort, splitErr := net.SplitHostPort(host); splitErr == nil {
		if parsedPort != "443" {
			return ""
		}
		hostname = parsedHost
	} else {
		hostname = strings.Trim(hostname, "[]")
		if hostname == "" || strings.Contains(hostname, ":") && net.ParseIP(hostname) == nil {
			return ""
		}
	}
	return net.JoinHostPort(hostname, port)
}

type authorizationCheck struct{ operation, resource string }

func authorizationChecks(req *spi.Request) []authorizationCheck {
	if req.ServiceID != "aws.s3" || req.Operation != "CopyObject" {
		return []authorizationCheck{{req.Operation, req.ServiceID + ":" + req.Operation}}
	}
	source := strValue(req.Input["CopySource"])
	if source == "" && req.HTTP != nil {
		source = req.HTTP.Header.Get("x-amz-copy-source")
	}
	source, _ = url.PathUnescape(strings.TrimPrefix(source, "/"))
	destination := strValue(req.Input["Bucket"]) + "/" + strValue(req.Input["Key"])
	checks := []authorizationCheck{
		{"GetObject", "arn:aws:s3:::" + source},
		{"PutObject", "arn:aws:s3:::" + destination},
	}
	directive := strValue(req.Input["TaggingDirective"])
	if directive == "" && req.HTTP != nil {
		directive = req.HTTP.Header.Get("x-amz-tagging-directive")
	}
	if strings.EqualFold(directive, "REPLACE") {
		checks = append(checks, authorizationCheck{"PutObjectTagging", "arn:aws:s3:::" + destination})
	}
	return checks
}

func strValue(value any) string {
	text, _ := value.(string)
	return text
}

func (s *Server) fault(w http.ResponseWriter, codec proto.Codec, svc *model.Service, op *model.Operation, err error, rid string) {
	f, ok := err.(*spi.Fault)
	if !ok {
		f = &spi.Fault{Code: "InternalError", Message: err.Error(), HTTPStatus: 500, Fault: "server"}
	}
	for key, values := range f.Headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if w.Header().Get("x-mirror-fidelity") == "" {
		w.Header().Set("x-mirror-fidelity", "mock")
	}
	_ = codec.EncodeFault(svc, op, w, f, rid)
}

func (s *Server) demux(r *http.Request) *model.Service {
	// Parsing the form is a side effect the packs depend on: S3's
	// SelectObjectContent and Route 53's health-check writes read `r.Form`,
	// which only exists because the demux populated it on the way past. It
	// happens before any answer is chosen so that no route can skip it.
	ct := strings.ToLower(r.Header.Get("Content-Type"))
	chunked := strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") || r.Header.Get("X-Amz-Decoded-Content-Length") != ""
	// Path-style S3 PUTs (incl. curl --data-binary, which defaults to form Content-Type)
	// must not ParseForm — that consumes the object body.
	s3PUT := r.Method == http.MethodPut && r.Header.Get("X-Amz-Target") == ""
	gcsBody := strings.Contains(r.URL.Path, "/storage/") || strings.Contains(r.URL.Path, "/upload/")
	if !s3PUT && !gcsBody && !chunked && strings.Contains(ct, "application/x-www-form-urlencoded") {
		_ = r.ParseForm()
	}
	action := r.URL.Query().Get("Action")
	if action == "" && r.Form != nil {
		action = r.Form.Get("Action")
	}
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, ".s3.") || strings.HasPrefix(host, "s3.") {
		return s.bundle.ServiceByID("aws.s3")
	}
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if strings.HasPrefix(target, "DynamoDBStreams_") {
			return s.bundle.ServiceByID("aws.dynamodb")
		}
		for i := range s.bundle.Services {
			svc := &s.bundle.Services[i]
			if svc.TargetPrefix != "" && strings.HasPrefix(target, svc.TargetPrefix) {
				return svc
			}
		}
		low := strings.ToLower(target)
		for i := range s.bundle.Services {
			svc := &s.bundle.Services[i]
			prefix := strings.ToLower(svc.EndpointPrefix)
			if prefix != "" && (low == prefix || strings.HasPrefix(low, prefix+".") || strings.HasPrefix(low, prefix+"_")) {
				return svc
			}
		}
	}
	// The host and the credential scope say which service this is for, and
	// the model already knows both, so this answers for every service in the
	// bundle. What it replaced was a chain of about a hundred and thirty
	// hand-written substring guesses, every one of them inside `if action !=
	// ""` -- a condition only a query-protocol request satisfies.
	if svc := s.resolveByModel(r); svc != nil {
		return svc
	}
	path := r.URL.Path
	if strings.Contains(path, "/storage/v1") || strings.Contains(path, "/upload/storage") {
		return s.bundle.ServiceByID("gcp.storage")
	}
	if strings.Contains(path, "/2015-03-31/") {
		return s.bundle.ServiceByID("aws.lambda")
	}
	if strings.HasPrefix(path, "/restapis") || strings.HasPrefix(path, "/apikeys") || strings.HasPrefix(path, "/usageplans") || strings.Contains(path, "/_user_request_") {
		return s.bundle.ServiceByID("aws.apigateway")
	}
	if strings.Contains(path, "/2013-04-01/") || strings.Contains(strings.ToLower(r.Host), "route53") {
		return s.bundle.ServiceByID("aws.route53")
	}
	if strings.Contains(path, "/2020-05-31/") || strings.Contains(strings.ToLower(r.Host), "cloudfront") {
		return s.bundle.ServiceByID("aws.cloudfront")
	}
	if s.looksLike(r, "eks") || strings.HasPrefix(path, "/clusters") && strings.Contains(strings.ToLower(r.Header.Get("Authorization")), "/eks/") {
		return s.bundle.ServiceByID("aws.eks")
	}
	if strings.Contains(path, "/_doc") || strings.Contains(path, "/_search") || strings.Contains(path, "/_aws/opensearch") || strings.Contains(path, "/2021-01-01/opensearch") {
		return s.bundle.ServiceByID("aws.es")
	}
	// default S3 path-style
	if r.Header.Get("X-Amz-Target") == "" && action == "" {
		return s.bundle.ServiceByID("aws.s3")
	}
	return nil
}

func (s *Server) looksLike(r *http.Request, prefix string) bool {
	host := strings.ToLower(r.Host)
	if strings.Contains(host, prefix) {
		return true
	}
	if strings.Contains(strings.ToLower(r.URL.Path), prefix) {
		return true
	}
	ak := r.Header.Get("Authorization")
	return strings.Contains(strings.ToLower(ak), "/"+prefix+"/")
}

func (s *Server) diag(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-mirror-fidelity", "emulate")
	switch {
	case r.URL.Path == "/_mirror/health" && r.Method == http.MethodGet:
		up := s.deps.Clock.Since(s.started).Milliseconds()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "uptime_ms": up, "version": s.version})
	case r.URL.Path == "/_mirror/services" && r.Method == http.MethodGet:
		var list []map[string]any
		for _, id := range s.reg.Enabled() {
			p, _ := s.reg.Resolve(id)
			svc := s.bundle.ServiceByID(id)
			item := map[string]any{"id": id, "operations": 0}
			if p != nil {
				item["tier"] = p.Tier()
				item["operations"] = len(p.Operations())
			}
			if svc != nil {
				item["protocol"] = svc.Protocol
			}
			list = append(list, item)
		}
		_ = json.NewEncoder(w).Encode(list)
	case r.URL.Path == "/_mirror/journal" && r.Method == http.MethodGet:
		q := r.URL.Query()
		ents := s.deps.Journal.Query(spi.Filter{ServiceID: q.Get("service"), Operation: q.Get("operation"), Limit: 100})
		_ = json.NewEncoder(w).Encode(ents)
	case strings.HasPrefix(r.URL.Path, "/_mirror/model/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(r.URL.Path, "/_mirror/model/")
		_ = json.NewEncoder(w).Encode(s.bundle.ServiceByID(id))
	case r.URL.Path == "/_mirror/clock/advance" && r.Method == http.MethodPost:
		var body struct {
			Duration string `json:"duration"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		d, _ := time.ParseDuration(body.Duration)
		err := s.deps.Clock.Advance(d)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		w.WriteHeader(204)
	case r.URL.Path == "/_mirror/reset" && r.Method == http.MethodPost:
		_ = s.deps.Store.Close()
		w.WriteHeader(204)
	default:
		http.NotFound(w, r)
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func (s *Server) serviceEnabled(id string) bool {
	if len(s.cfg.Services) == 0 {
		return true
	}
	for _, x := range s.cfg.Services {
		if x == id {
			return true
		}
	}
	return false
}

// Handler is the http.Handler for tests.
func (s *Server) Handler() http.Handler { return s }
