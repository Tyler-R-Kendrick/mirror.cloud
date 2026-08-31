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
	awsChunkedInvalid := false
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
	if action != "" {
		if s.looksLike(r, "s3-control") || s.looksLike(r, "s3control") {
			return s.bundle.ServiceByID("aws.s3control")
		}
		if s.looksLike(r, "s3tables") {
			return s.bundle.ServiceByID("aws.s3tables")
		}
		if s.looksLike(r, "s3") {
			return s.bundle.ServiceByID("aws.s3")
		}
		// STS/SNS/IAM/SQS query
		if s.looksLike(r, "sts") {
			return s.bundle.ServiceByID("aws.sts")
		}
		if s.looksLike(r, "sns") {
			return s.bundle.ServiceByID("aws.sns")
		}
		if s.looksLike(r, "iam") {
			return s.bundle.ServiceByID("aws.iam")
		}
		if s.looksLike(r, "sqs") {
			return s.bundle.ServiceByID("aws.sqs")
		}
		if s.looksLike(r, "cloudformation") {
			return s.bundle.ServiceByID("aws.cloudformation")
		}
		if s.looksLike(r, "monitoring") {
			return s.bundle.ServiceByID("aws.monitoring")
		}
		if s.looksLike(r, "rds") {
			return s.bundle.ServiceByID("aws.rds")
		}
		if s.looksLike(r, "docdb") {
			return s.bundle.ServiceByID("aws.docdb")
		}
		if s.looksLike(r, "neptune") {
			return s.bundle.ServiceByID("aws.neptune")
		}
		if s.looksLike(r, "elasticloadbalancing") {
			return s.bundle.ServiceByID("aws.elasticloadbalancing")
		}
		if s.looksLike(r, "elasticache") {
			return s.bundle.ServiceByID("aws.elasticache")
		}
		if s.looksLike(r, "autoscaling") {
			return s.bundle.ServiceByID("aws.autoscaling")
		}
		if s.looksLike(r, "redshift") {
			return s.bundle.ServiceByID("aws.redshift")
		}
		if s.looksLike(r, "lambda") {
			return s.bundle.ServiceByID("aws.lambda")
		}
		if s.looksLike(r, "apigateway") {
			return s.bundle.ServiceByID("aws.apigateway")
		}
		if s.looksLike(r, "route53resolver") {
			return s.bundle.ServiceByID("aws.route53resolver")
		}
		if s.looksLike(r, "route53") {
			return s.bundle.ServiceByID("aws.route53")
		}
		if s.looksLike(r, "ec2") {
			return s.bundle.ServiceByID("aws.ec2")
		}
		if s.looksLike(r, "ses") || s.looksLike(r, "email") {
			return s.bundle.ServiceByID("aws.ses")
		}
		if s.looksLike(r, "cognito-idp") {
			return s.bundle.ServiceByID("aws.cognito-idp")
		}
		if s.looksLike(r, "cloudfront") {
			return s.bundle.ServiceByID("aws.cloudfront")
		}
		if s.looksLike(r, "elasticsearch") {
			return s.bundle.ServiceByID("aws.elasticsearch")
		}
		if s.looksLike(r, "es") || s.looksLike(r, "opensearch") {
			return s.bundle.ServiceByID("aws.es")
		}
		if s.looksLike(r, "glue") {
			return s.bundle.ServiceByID("aws.glue")
		}
		if s.looksLike(r, "athena") {
			return s.bundle.ServiceByID("aws.athena")
		}
		if s.looksLike(r, "cloudtrail") {
			return s.bundle.ServiceByID("aws.cloudtrail")
		}
		if s.looksLike(r, "organizations") {
			return s.bundle.ServiceByID("aws.organizations")
		}
		if s.looksLike(r, "config") {
			return s.bundle.ServiceByID("aws.config")
		}
		if s.looksLike(r, "xray") {
			return s.bundle.ServiceByID("aws.xray")
		}
		if s.looksLike(r, "guardduty") {
			return s.bundle.ServiceByID("aws.guardduty")
		}
		if s.looksLike(r, "mq") {
			return s.bundle.ServiceByID("aws.mq")
		}
		if s.looksLike(r, "iotwireless") {
			return s.bundle.ServiceByID("aws.iotwireless")
		}
		if s.looksLike(r, "iotdata") || s.looksLike(r, "iot-data") || s.looksLike(r, "data.iot") {
			return s.bundle.ServiceByID("aws.iot-data")
		}
		if s.looksLike(r, "iot") {
			return s.bundle.ServiceByID("aws.iot")
		}
		if s.looksLike(r, "pipes") {
			return s.bundle.ServiceByID("aws.pipes")
		}
		if s.looksLike(r, "codepipeline") {
			return s.bundle.ServiceByID("aws.codepipeline")
		}
		if s.looksLike(r, "appsync") {
			return s.bundle.ServiceByID("aws.appsync")
		}
		if s.looksLike(r, "apigatewayv2") {
			return s.bundle.ServiceByID("aws.apigatewayv2")
		}
		if s.looksLike(r, "codecommit") {
			return s.bundle.ServiceByID("aws.codecommit")
		}
		if s.looksLike(r, "codedeploy") {
			return s.bundle.ServiceByID("aws.codedeploy")
		}
		if s.looksLike(r, "amplify") {
			return s.bundle.ServiceByID("aws.amplify")
		}
		if s.looksLike(r, "inspector") {
			return s.bundle.ServiceByID("aws.inspector")
		}
		if s.looksLike(r, "securityhub") {
			return s.bundle.ServiceByID("aws.securityhub")
		}
		if s.looksLike(r, "timestream") {
			return s.bundle.ServiceByID("aws.timestream")
		}
		if s.looksLike(r, "qldb") {
			return s.bundle.ServiceByID("aws.qldb")
		}
		if s.looksLike(r, "dms") {
			return s.bundle.ServiceByID("aws.dms")
		}
		if s.looksLike(r, "mediaconvert") {
			return s.bundle.ServiceByID("aws.mediaconvert")
		}
		if s.looksLike(r, "elasticbeanstalk") {
			return s.bundle.ServiceByID("aws.elasticbeanstalk")
		}
		if s.looksLike(r, "swf") {
			return s.bundle.ServiceByID("aws.swf")
		}
		if s.looksLike(r, "elasticfilesystem") || s.looksLike(r, "efs") {
			return s.bundle.ServiceByID("aws.elasticfilesystem")
		}
		if s.looksLike(r, "glacier") {
			return s.bundle.ServiceByID("aws.glacier")
		}
		if s.looksLike(r, "servicediscovery") {
			return s.bundle.ServiceByID("aws.servicediscovery")
		}
		if s.looksLike(r, "ram") {
			return s.bundle.ServiceByID("aws.ram")
		}
		if s.looksLike(r, "sagemaker") {
			return s.bundle.ServiceByID("aws.sagemaker")
		}
		if s.looksLike(r, "workspaces") {
			return s.bundle.ServiceByID("aws.workspaces")
		}
		if s.looksLike(r, "transcribe") {
			return s.bundle.ServiceByID("aws.transcribe")
		}
		if s.looksLike(r, "rekognition") {
			return s.bundle.ServiceByID("aws.rekognition")
		}
		if s.looksLike(r, "comprehendmedical") {
			return s.bundle.ServiceByID("aws.comprehendmedical")
		}
		if s.looksLike(r, "comprehend") {
			return s.bundle.ServiceByID("aws.comprehend")
		}
		if s.looksLike(r, "mediastore") {
			return s.bundle.ServiceByID("aws.mediastore")
		}
		if s.looksLike(r, "kinesisanalyticsv2") {
			return s.bundle.ServiceByID("aws.kinesisanalyticsv2")
		}
		if s.looksLike(r, "kinesisanalytics") {
			return s.bundle.ServiceByID("aws.kinesisanalytics")
		}
		if s.looksLike(r, "translate") {
			return s.bundle.ServiceByID("aws.translate")
		}
		if s.looksLike(r, "textract") {
			return s.bundle.ServiceByID("aws.textract")
		}
		if s.looksLike(r, "polly") {
			return s.bundle.ServiceByID("aws.polly")
		}
		if s.looksLike(r, "fsx") {
			return s.bundle.ServiceByID("aws.fsx")
		}
		if s.looksLike(r, "servicecatalog") {
			return s.bundle.ServiceByID("aws.servicecatalog")
		}
		if s.looksLike(r, "shield") {
			return s.bundle.ServiceByID("aws.shield")
		}
		if s.looksLike(r, "wafv2") {
			return s.bundle.ServiceByID("aws.wafv2")
		}
		if s.looksLike(r, "waf") {
			return s.bundle.ServiceByID("aws.waf")
		}
		if s.looksLike(r, "storagegateway") {
			return s.bundle.ServiceByID("aws.storagegateway")
		}
		if s.looksLike(r, "lakeformation") {
			return s.bundle.ServiceByID("aws.lakeformation")
		}
		if s.looksLike(r, "connect") {
			return s.bundle.ServiceByID("aws.connect")
		}
		if s.looksLike(r, "pinpoint") || s.looksLike(r, "mobiletargeting") {
			return s.bundle.ServiceByID("aws.pinpoint")
		}
		if s.looksLike(r, "dax") {
			return s.bundle.ServiceByID("aws.dax")
		}
		if s.looksLike(r, "memorydb") {
			return s.bundle.ServiceByID("aws.memorydb")
		}
		if s.looksLike(r, "keyspaces") || s.looksLike(r, "cassandra") {
			return s.bundle.ServiceByID("aws.keyspaces")
		}
		if s.looksLike(r, "mwaa") || s.looksLike(r, "airflow") {
			return s.bundle.ServiceByID("aws.mwaa")
		}
		if s.looksLike(r, "sso-admin") || s.looksLike(r, "ssoadmin") || s.looksLike(r, "sso") {
			return s.bundle.ServiceByID("aws.sso-admin")
		}
		if s.looksLike(r, "acm-pca") || s.looksLike(r, "acmpca") {
			return s.bundle.ServiceByID("aws.acm-pca")
		}
		if s.looksLike(r, "lightsail") {
			return s.bundle.ServiceByID("aws.lightsail")
		}
		if s.looksLike(r, "location") || s.looksLike(r, "geo") {
			return s.bundle.ServiceByID("aws.location")
		}
		if s.looksLike(r, "kendra") {
			return s.bundle.ServiceByID("aws.kendra")
		}
		if s.looksLike(r, "quicksight") {
			return s.bundle.ServiceByID("aws.quicksight")
		}
		if s.looksLike(r, "identitystore") {
			return s.bundle.ServiceByID("aws.identitystore")
		}
		if s.looksLike(r, "workmail") {
			return s.bundle.ServiceByID("aws.workmail")
		}
		if s.looksLike(r, "directconnect") {
			return s.bundle.ServiceByID("aws.directconnect")
		}
		if s.looksLike(r, "directoryservice") {
			return s.bundle.ServiceByID("aws.ds")
		}
		if s.looksLike(r, "gamelift") {
			return s.bundle.ServiceByID("aws.gamelift")
		}
		if s.looksLike(r, "forecast") {
			return s.bundle.ServiceByID("aws.forecast")
		}
		if s.looksLike(r, "personalize") {
			return s.bundle.ServiceByID("aws.personalize")
		}
		if s.looksLike(r, "lex-models") || s.looksLike(r, "lex") {
			return s.bundle.ServiceByID("aws.lex-models")
		}
		if s.looksLike(r, "medialive") {
			return s.bundle.ServiceByID("aws.medialive")
		}
		if s.looksLike(r, "mediapackage") {
			return s.bundle.ServiceByID("aws.mediapackage")
		}
		if s.looksLike(r, "mediaconnect") {
			return s.bundle.ServiceByID("aws.mediaconnect")
		}
		if s.looksLike(r, "elastictranscoder") {
			return s.bundle.ServiceByID("aws.elastictranscoder")
		}
		if s.looksLike(r, "cloudhsmv2") || s.looksLike(r, "cloudhsm") {
			return s.bundle.ServiceByID("aws.cloudhsmv2")
		}
		if s.looksLike(r, "macie2") || s.looksLike(r, "macie") {
			return s.bundle.ServiceByID("aws.macie2")
		}
		if s.looksLike(r, "access-analyzer") || s.looksLike(r, "accessanalyzer") {
			return s.bundle.ServiceByID("aws.access-analyzer")
		}
		if s.looksLike(r, "frauddetector") {
			return s.bundle.ServiceByID("aws.frauddetector")
		}
		if s.looksLike(r, "appmesh") {
			return s.bundle.ServiceByID("aws.appmesh")
		}
		if s.looksLike(r, "healthlake") {
			return s.bundle.ServiceByID("aws.healthlake")
		}
		if s.looksLike(r, "lookoutmetrics") {
			return s.bundle.ServiceByID("aws.lookoutmetrics")
		}
		if s.looksLike(r, "bedrock") {
			return s.bundle.ServiceByID("aws.bedrock")
		}
		if s.looksLike(r, "fis") {
			return s.bundle.ServiceByID("aws.fis")
		}
		if strings.Contains(strings.ToLower(r.Host), "ce.") || strings.Contains(strings.ToLower(r.Header.Get("Authorization")), "/ce/") {
			return s.bundle.ServiceByID("aws.ce")
		}
		if s.looksLike(r, "resource-groups") || s.looksLike(r, "resourcegroups") {
			return s.bundle.ServiceByID("aws.resource-groups")
		}
		if s.looksLike(r, "verifiedpermissions") {
			return s.bundle.ServiceByID("aws.verifiedpermissions")
		}
		if s.looksLike(r, "support") {
			return s.bundle.ServiceByID("aws.support")
		}
		if s.looksLike(r, "codeartifact") {
			return s.bundle.ServiceByID("aws.codeartifact")
		}
		if s.looksLike(r, "cloudcontrol") || s.looksLike(r, "cloudcontrolapi") {
			return s.bundle.ServiceByID("aws.cloudcontrol")
		}
		if s.looksLike(r, "serverlessrepo") {
			return s.bundle.ServiceByID("aws.serverlessrepo")
		}
		if s.looksLike(r, "account") {
			return s.bundle.ServiceByID("aws.account")
		}
		if s.looksLike(r, "iotwireless") {
			return s.bundle.ServiceByID("aws.iotwireless")
		}
		if s.looksLike(r, "s3tables") {
			return s.bundle.ServiceByID("aws.s3tables")
		}
		if s.looksLike(r, "synthetics") {
			return s.bundle.ServiceByID("aws.synthetics")
		}
		if s.looksLike(r, "apprunner") {
			return s.bundle.ServiceByID("aws.apprunner")
		}
		if s.looksLike(r, "proton") {
			return s.bundle.ServiceByID("aws.proton")
		}
		if s.looksLike(r, "resiliencehub") {
			return s.bundle.ServiceByID("aws.resiliencehub")
		}
		if s.looksLike(r, "resource-explorer-2") || s.looksLike(r, "resource-explorer") {
			return s.bundle.ServiceByID("aws.resource-explorer-2")
		}
		if s.looksLike(r, "rum") {
			return s.bundle.ServiceByID("aws.rum")
		}
		if s.looksLike(r, "schemas") {
			return s.bundle.ServiceByID("aws.schemas")
		}
		if s.looksLike(r, "dsql") {
			return s.bundle.ServiceByID("aws.dsql")
		}
		if s.looksLike(r, "codeconnections") {
			return s.bundle.ServiceByID("aws.codeconnections")
		}
		if s.looksLike(r, "iotdata") || s.looksLike(r, "iot-data") || s.looksLike(r, "data.iot") {
			return s.bundle.ServiceByID("aws.iot-data")
		}
		if s.looksLike(r, "managedblockchain") {
			return s.bundle.ServiceByID("aws.managedblockchain")
		}
		if s.looksLike(r, "kinesisanalyticsv2") {
			return s.bundle.ServiceByID("aws.kinesisanalyticsv2")
		}
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
