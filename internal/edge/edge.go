// Package edge is the HTTP demux: identity, codecs, packs, diagnostics.
package edge

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/awsjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/awsquery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/gcprest"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/restjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/restxml"
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
		body, err := deframeAWSChunked(r.Body)
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}
	}

	id := identity.Parse(r, s.cfg.DefaultAccount, s.cfg.DefaultRegion, s.deps.Clock.Now())
	if identity.Expired(id) {
		http.Error(w, "Request has expired", http.StatusForbidden)
		return
	}

	svc := s.demux(r)
	w.Header().Set("x-mirror-request-id", rid)
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

func (s *Server) fault(w http.ResponseWriter, codec proto.Codec, svc *model.Service, op *model.Operation, err error, rid string) {
	f, ok := err.(*spi.Fault)
	if !ok {
		f = &spi.Fault{Code: "InternalError", Message: err.Error(), HTTPStatus: 500, Fault: "server"}
	}
	w.Header().Set("x-mirror-fidelity", "emulate")
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
		low := strings.ToLower(target)
		switch {
		case strings.Contains(low, "dynamodb"):
			return s.bundle.ServiceByID("aws.dynamodb")
		case strings.Contains(low, "amazonsqs") || strings.Contains(low, "sqs"):
			return s.bundle.ServiceByID("aws.sqs")
		case strings.Contains(low, "amazonssm") || strings.Contains(low, "ssm"):
			return s.bundle.ServiceByID("aws.ssm")
		case strings.Contains(low, "secretsmanager"):
			return s.bundle.ServiceByID("aws.secretsmanager")
		}
	}
	_ = r.ParseForm()
	if a := r.Form.Get("Action"); a != "" {
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
	}
	path := r.URL.Path
	if strings.Contains(path, "/storage/v1") || strings.Contains(path, "/upload/storage") {
		return s.bundle.ServiceByID("gcp.storage")
	}
	// default S3 path-style
	if r.Header.Get("X-Amz-Target") == "" && r.Form.Get("Action") == "" {
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
