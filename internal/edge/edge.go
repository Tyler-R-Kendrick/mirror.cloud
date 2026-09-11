// Package edge is the HTTP demux: identity, codecs, packs, diagnostics.
package edge

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns/cert"
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
	if s.handleAWSInternal(w, r) {
		return
	}
	var awsChunks [][]byte
	var awsChunkSignatures []string
	var awsTrailers http.Header
	var awsDecodedLength int64
	awsChunkedDecoded := false
	awsChunkedInvalid := false
	if r.Method == http.MethodOptions {
		if svc := s.demux(r); svc == nil || svc.ID != "aws.s3" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.WriteHeader(204)
			return
		}
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
		decodedLength, lengthErr := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
		if err == nil && lengthErr == nil && decodedLength >= 0 {
			body := raw
			if deframed, chunks, signatures, trailers, err2 := parseAWSChunked(bytes.NewReader(raw)); err2 == nil {
				if int64(len(deframed)) == decodedLength {
					body = deframed
					awsChunks = chunks
					awsChunkSignatures = signatures
					awsTrailers = trailers
					awsDecodedLength = int64(len(body))
					awsChunkedDecoded = true
				} else {
					awsChunkedInvalid = true
				}
			} else {
				awsChunkedInvalid = true
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		} else {
			awsChunkedInvalid = true
			r.Body = io.NopCloser(bytes.NewReader(raw))
		}
	}

	svc := s.demux(r)
	w.Header().Set("x-mirror-request-id", rid)
	if svc != nil && svc.ID == "aws.s3" && awsChunkedInvalid {
		operation := "unknown"
		fault := &spi.Fault{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", HTTPStatus: http.StatusForbidden, Fault: "client"}
		if r.Method == http.MethodPut && r.URL.Query().Get("partNumber") != "" && r.URL.Query().Get("uploadId") != "" {
			operation = "UploadPart"
			fault = &spi.Fault{Code: "InternalError", Message: "We encountered an internal error. Please try again.", HTTPStatus: http.StatusInternalServerError, Fault: "server"}
		}
		s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: operation}, fault, rid)
		return
	}
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
			fault = identity.VerifyS3StreamingSignature(r, id.AccessKeyID, secret, awsChunks, awsChunkSignatures, awsTrailers)
		}
		if fault != nil {
			s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "unknown"}, fault, rid)
			return
		}
	}
	if awsChunkedDecoded {
		r.ContentLength = awsDecodedLength
		var encodings []string
		for _, encoding := range strings.Split(r.Header.Get("Content-Encoding"), ",") {
			if encoding = strings.TrimSpace(encoding); encoding != "" && !strings.EqualFold(encoding, "aws-chunked") {
				encodings = append(encodings, encoding)
			}
		}
		if len(encodings) == 0 {
			r.Header.Del("Content-Encoding")
		} else {
			r.Header.Set("Content-Encoding", strings.Join(encodings, ","))
		}
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
			codec = awsquery.Codec{JSON: awsquery.WantsJSON(r)}
			if err := r.ParseForm(); err == nil {
				action := r.Form.Get("Action")
				if action == "" {
					// Answered before a codec sees it, because there is no
					// operation to route to and no envelope to put a modelled
					// fault in. See sqsUnknownOperation.
					w.Header().Set("x-mirror-fidelity", "emulate")
					w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
					w.WriteHeader(http.StatusNotFound)
					_, _ = io.WriteString(w, sqsUnknownOperation)
					return
				}
				if sqsQueuePath(r.URL.Path) {
					if err := sqsQueueEndpointAction(svc, action); err != nil {
						s.fault(w, codec, svc, &model.Operation{Name: action}, err, rid)
						return
					}
				}
			}
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
	// The configured advertise URL reaches packs here, which is the one place
	// every decoded request passes through. It used to reach the Server struct
	// and stop: `advertise` was assigned in New and read nowhere, so
	// `--advertise-url` was accepted, documented and silently discarded, and
	// four packs that ask for it (sqs, sns signing, cloudformation, s3
	// location) always fell through to the request's own Host. A self-
	// referencing URL that names the container's address is exactly the case
	// the flag exists to fix.
	req.AdvertiseURL = s.advertise

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
	if action != "" {
		if sqsQueuePath(r.URL.Path) || sqsQueueDomainHost(host) {
			return s.bundle.ServiceByID("aws.sqs")
		}
		if action == "ConfirmSubscription" || action == "Unsubscribe" {
			arn := r.URL.Query().Get("TopicArn")
			if action == "Unsubscribe" {
				arn = r.URL.Query().Get("SubscriptionArn")
			}
			parts := strings.Split(arn, ":")
			if len(parts) >= 6 && parts[0] == "arn" && parts[2] == "sns" {
				return s.bundle.ServiceByID("aws.sns")
			}
		}
	}
	// The provider guesses below claim a path rather than an endpoint, and a
	// path is not theirs to claim when the request has already said which AWS
	// service it is for. `flyRequest` is true of any path containing
	// `/v1/apps`, which is also AWS Pinpoint's GetApps, so a signed Pinpoint
	// request was answered by Fly. This is the third time a new provider has
	// taken an AWS service this way -- Vercel took ECR and IoT Wireless by
	// name, Fly took Pinpoint by path -- so the guard is on the class rather
	// than on the instance.
	//
	// An SDK says which service it means in ways a provider client never does:
	// a SigV4 credential scope, an X-Amz-Target, an AWS endpoint host. Where
	// one of those is present and the model can place it, the model wins.
	if !awsAddressed(r) || s.resolveByModel(r) == nil {
		if flyRequest(r) {
			return s.bundle.ServiceByID("fly.machines")
		}
		if railwayRequest(r) {
			return s.bundle.ServiceByID("railway.graphql")
		}
		if hetznerRequest(r) {
			return s.bundle.ServiceByID("hetzner.v1")
		}
		if digitaloceanRequest(r) {
			return s.bundle.ServiceByID("digitalocean.v2")
		}
		if id := azureService(r); id != "" {
			return s.bundle.ServiceByID(id)
		}
		if cloudflareRequest(r) {
			return s.bundle.ServiceByID("cloudflare.api")
		}
		if hostingerRequest(r) {
			return s.bundle.ServiceByID("hostinger.api")
		}
		if vercelRequest(r) {
			return s.bundle.ServiceByID("vercel.api")
		}
	}
	if svc := s.resolveByModel(r); svc != nil {
		return svc
	}
	if action == "" && r.Method == http.MethodGet && sqsQueuePath(r.URL.Path) {
		return s.bundle.ServiceByID("aws.sqs")
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

// awsAddressed reports whether the request names an AWS service the way an AWS
// client does. A provider client sends none of these: Fly, Vercel and the rest
// authenticate with a bearer token and address a bare host.
func awsAddressed(r *http.Request) bool {
	if r.Header.Get("X-Amz-Target") != "" {
		return true
	}
	if credentialScopeService(r.Header.Get("Authorization")) != "" {
		return true
	}
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	return strings.HasSuffix(host, ".amazonaws.com") || strings.HasSuffix(host, ".api.aws")
}

func flyRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "machines.dev") {
		return true
	}
	return strings.Contains(r.URL.Path, "/v1/apps")
}

func railwayRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "railway") {
		return true
	}
	return strings.Contains(r.URL.Path, "/graphql/v2")
}

func hetznerRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "hetzner") {
		return true
	}
	path := r.URL.Path
	return strings.HasPrefix(path, "/v1/servers") || strings.HasPrefix(path, "/v1/ssh_keys")
}

func digitaloceanRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "digitalocean") {
		return true
	}
	path := r.URL.Path
	return strings.HasPrefix(path, "/v2/droplets") || strings.HasPrefix(path, "/v2/domains")
}

func azureService(r *http.Request) string {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "queue.core.windows.net") {
		return "azure.queue"
	}
	if strings.Contains(host, "table.core.windows.net") {
		return "azure.table"
	}
	if azureRequest(r) {
		return "azure.blobs"
	}
	return ""
}

func azureRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "blob.core.windows.net") || strings.Contains(host, "azure") {
		return true
	}
	q := r.URL.Query()
	return q.Get("restype") == "container" || q.Get("comp") == "list" || r.Header.Get("x-ms-blob-type") != ""
}

func hostingerRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "hostinger") {
		return true
	}
	path := r.URL.Path
	return strings.HasPrefix(path, "/api/dns/") || strings.HasPrefix(path, "/api/domains/")
}

func cloudflareRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "cloudflare") {
		return true
	}
	return strings.Contains(r.URL.Path, "/client/v4/")
}

func vercelRequest(r *http.Request) bool {
	host := strings.ToLower(r.Host)
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if strings.Contains(host, "vercel") {
		return true
	}
	path := r.URL.Path
	if len(path) < 4 || path[0] != '/' || path[1] != 'v' || path[2] < '1' || path[2] > '9' {
		return false
	}
	return strings.Contains(path, "/projects") || strings.Contains(path, "/deployments") || strings.Contains(path, "/user") || strings.Contains(path, "/teams")
}

func sqsQueuePath(path string) bool {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	account, name := "", ""
	switch {
	case len(parts) == 2:
		account, name = parts[0], parts[1]
	case len(parts) == 4 && parts[0] == "queue":
		account, name = parts[2], parts[3]
	default:
		return false
	}
	if len(account) != 12 || name == "" {
		return false
	}
	for _, r := range account {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func sqsQueueDomainHost(host string) bool {
	return strings.HasPrefix(host, "queue.") || strings.Contains(host, ".queue.")
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

func (s *Server) handleAWSInternal(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	if r.Method == http.MethodDelete && r.URL.Path == "/_aws/dynamodb/expired" {
		s.expireDynamoDBItems(w, r)
		return true
	}
	if strings.Contains(path, "/_aws/sns/SimpleNotificationService") && strings.HasSuffix(path, ".pem") {
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(cert.Certificate())
		return true
	}
	switch {
	case path == "/_aws/ses" || strings.HasPrefix(path, "/_aws/ses/"):
		s.sesMailbox(w, r)
		return true
	case strings.HasPrefix(path, "/_aws/sns/phone-opt-outs"):
		s.snsPhoneOptOut(w, r)
		return true
	case strings.HasPrefix(path, "/_aws/sns/sms-messages"):
		s.snsJSONList(w, r, "snssms", "sms_messages", "phoneNumber")
		return true
	case strings.HasPrefix(path, "/_aws/sns/platform-endpoint-messages"):
		s.snsJSONList(w, r, "snsplat", "platform_endpoint_messages", "endpointArn")
		return true
	case strings.HasPrefix(path, "/_aws/sns/subscription-tokens"):
		s.snsSubscriptionToken(w, r)
		return true
	}
	return false
}

func (s *Server) expireDynamoDBItems(w http.ResponseWriter, r *http.Request) {
	const serviceID = "aws.dynamodb"
	pack, ok := s.reg.Resolve(serviceID)
	if !ok || pack == nil || !s.serviceEnabled(serviceID) {
		http.Error(w, "MirrorNotImplemented: aws.dynamodb", http.StatusNotImplemented)
		return
	}
	id := s.internalIdentity(r)
	response, err := pack.Invoke(r.Context(), &spi.Request{Identity: id, ServiceID: serviceID, Operation: "ExpireItems", HTTP: r})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-mirror-fidelity", string(pack.Tier()))
	_ = json.NewEncoder(w).Encode(response.Output)
}

func (s *Server) internalIdentity(r *http.Request) spi.Identity {
	q := r.URL.Query()
	account, region := q.Get("accountId"), q.Get("region")
	if account == "" {
		account = s.cfg.DefaultAccount
	}
	if region == "" {
		region = s.cfg.DefaultRegion
		if region == "" {
			region = "us-east-1"
		}
	}
	return spi.Identity{Account: account, Region: region}
}

func (s *Server) sesMailbox(w http.ResponseWriter, r *http.Request) {
	id := s.internalIdentity(r)
	col := s.deps.Store.Scope(id.Account, id.Region).Collection("sesmsg")
	switch r.Method {
	case http.MethodDelete:
		kvs, _, _ := col.List(r.Context(), "", "", 0)
		for _, kv := range kvs {
			_ = col.Delete(r.Context(), kv.Key)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		kvs, _, _ := col.List(r.Context(), "", "", 0)
		var messages []any
		for _, kv := range kvs {
			var rec map[string]any
			if json.Unmarshal(kv.Value, &rec) == nil {
				messages = append(messages, rec)
			}
		}
		if messages == nil {
			messages = []any{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": messages})
	}
}

func (s *Server) snsPhoneOptOut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	phone, _ := body["phoneNumber"].(string)
	account, _ := body["accountId"].(string)
	if phone == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if account == "" {
		account = s.internalIdentity(r).Account
	}
	region := r.URL.Query().Get("region")
	if region == "" {
		region = s.cfg.DefaultRegion
		if region == "" {
			region = "us-east-1"
		}
	}
	_ = s.deps.Store.Scope(account, region).Collection("smsopt").Put(r.Context(), phone, []byte("true"))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) snsJSONList(w http.ResponseWriter, r *http.Request, colName, field, filterKey string) {
	id := s.internalIdentity(r)
	col := s.deps.Store.Scope(id.Account, id.Region).Collection(colName)
	filter := r.URL.Query().Get(filterKey)
	switch r.Method {
	case http.MethodDelete:
		if filter != "" {
			_ = col.Delete(r.Context(), filter)
		} else {
			kvs, _, _ := col.List(r.Context(), "", "", 0)
			for _, kv := range kvs {
				_ = col.Delete(r.Context(), kv.Key)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		kvs, _, _ := col.List(r.Context(), "", "", 0)
		out := map[string]any{}
		for _, kv := range kvs {
			if filter != "" && kv.Key != filter {
				continue
			}
			var list []any
			_ = json.Unmarshal(kv.Value, &list)
			out[kv.Key] = list
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{field: out, "region": id.Region})
	}
}

func (s *Server) snsSubscriptionToken(w http.ResponseWriter, r *http.Request) {
	arn := strings.TrimPrefix(r.URL.Path, "/_aws/sns/subscription-tokens/")
	w.Header().Set("Content-Type", "application/json")
	if !strings.HasPrefix(arn, "arn:") || strings.Count(arn, ":") < 5 {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "The provided SubscriptionARN is invalid", "subscription_arn": arn})
		return
	}
	parts := strings.Split(arn, ":")
	id := s.internalIdentity(r)
	if len(parts) > 3 && parts[3] != "" {
		id.Region = parts[3]
	}
	if len(parts) > 4 && parts[4] != "" {
		id.Account = parts[4]
	}
	b, ok, _ := s.deps.Store.Scope(id.Account, id.Region).Collection("subs").Get(r.Context(), arn)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "The provided SubscriptionARN is not found", "subscription_arn": arn})
		return
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	_ = json.NewEncoder(w).Encode(map[string]any{"subscription_token": rec["Token"], "subscription_arn": arn})
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
