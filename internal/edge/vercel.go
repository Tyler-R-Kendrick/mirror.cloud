package edge

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// What follows is the part of api.vercel.com that happens before an operation
// is chosen: whether the caller is who the request says, whether it has been
// told to slow down, and which team it is acting for. None of it is behavior of
// any one operation, so none of it belongs in the bundle, and none of it
// decides anything the engine would otherwise get right.

const vercelService = "vercel.api"

// vercelControlPrefix is the test-control surface. Everything under it is
// mirror's own, not Vercel's, and exists only when Config.VercelFaults is set.
const vercelControlPrefix = "/_mirror/vercel/"

// vercelTeamSlugs is the global collection CreateTeam writes a team's slug to,
// keyed by slug with the team's id as the record's teamId. The edge reads it to
// turn ?slug= into a team; behavior/vercel/api declares the resource global so
// that the edge, which has no account to look in, can find it, and
// internal/check holds the two to the same name.
const vercelTeamSlugs = "vteamslug"

// vercelLimiter answers 429 to the next n requests. It is process-local and
// armed only through the control route.
type vercelLimiter struct {
	mu         sync.Mutex
	remaining  int
	total      int
	retryAfter int
}

func (l *vercelLimiter) arm(n, retryAfter int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.remaining, l.total, l.retryAfter = n, n, retryAfter
}

// take reports whether this request is one to refuse, and the limit that
// refuses it.
func (l *vercelLimiter) take() (retryAfter, total int, refuse bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.remaining <= 0 {
		return 0, 0, false
	}
	l.remaining--
	return l.retryAfter, l.total, true
}

// vercelControlOperation reports whether a request is for a control route the
// bundle serves as an operation, so that it rides the ordinary pipeline (and
// the engine's statechart) instead of being answered by diag.
func (s *Server) vercelControlOperation(r *http.Request) bool {
	return s.cfg.VercelFaults &&
		strings.HasPrefix(r.URL.Path, vercelControlPrefix) &&
		r.URL.Path != vercelControlPrefix+"rate-limit"
}

// vercelRateLimit is POST /_mirror/vercel/rate-limit {"requests": n,
// "retryAfter": seconds}: the next n requests to vercel.api answer 429.
// DELETE disarms it.
func (s *Server) vercelRateLimit(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		s.vercelLimit.arm(0, 0)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		var body struct {
			Requests   int `json:"requests"`
			RetryAfter int `json:"retryAfter"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Requests < 1 {
			http.Error(w, `{"error":{"code":"bad_request","message":"requests must be a positive integer"}}`, http.StatusBadRequest)
			return
		}
		if body.RetryAfter < 1 {
			body.RetryAfter = 1
		}
		s.vercelLimit.arm(body.Requests, body.RetryAfter)
		_ = json.NewEncoder(w).Encode(map[string]any{"requests": body.Requests, "retryAfter": body.RetryAfter})
	default:
		http.NotFound(w, r)
	}
}

// vercelPrologue runs ahead of routing for a request vercel.api will serve. It
// answers the request itself (and reports true) when it is refused, and
// otherwise rewrites the identity to the team the request names.
func (s *Server) vercelPrologue(w http.ResponseWriter, r *http.Request, id *spi.Identity, svc *model.Service, rid string) bool {
	refuse := func(f *spi.Fault) bool {
		s.fault(w, s.codecs[svc.Protocol], svc, &model.Operation{Name: "unknown"}, f, rid)
		return true
	}
	// The control surface is mirror's, not the API's: it takes no credential
	// and is never rate limited, but it acts on the team it names like
	// everything else does, or a test could not break a team's deployments.
	if !strings.HasPrefix(r.URL.Path, vercelControlPrefix) {
		// The covenant says any credentials work, and by default none at all
		// do. A client's own handling of "you are not signed in" is worth
		// testing, so a deployment can ask for Vercel's refusals; they are the
		// real ones (the CLI keys on the code and on invalidToken), not a new
		// shape.
		if s.cfg.VercelStrictAuth {
			if f := vercelAuthFault(r); f != nil {
				return refuse(f)
			}
		}
		if retryAfter, total, limited := s.vercelLimit.take(); limited {
			return refuse(vercelRateLimited(s.deps.Clock.Now().Unix(), retryAfter, total))
		}
	}
	// A request acts for a team when it says so, by id or by slug; otherwise it
	// acts for the caller's own account. The engine scopes every record by the
	// identity's account, so rewriting it is the whole of team isolation: a
	// project made for one team is not in another's listing, and a name one
	// team holds is free in the next. The caller's own account is kept in
	// Project, which the engine exposes as identity.project.
	q := r.URL.Query()
	id.Project = id.Account
	if team := q.Get("teamId"); team != "" {
		// The caller's own id is the personal account, not a team of that
		// name: the CLI writes the user's id as the org of a linked project
		// and `vercel ls` sends it back as ?teamId=. Acting for an empty team
		// of that id listed nothing the caller had just deployed.
		if team != "usr_"+id.Account && team != id.Account {
			id.Account = team
		}
		return false
	}
	if slug := q.Get("slug"); slug != "" {
		team, ok := s.vercelTeamBySlug(r, slug)
		if !ok {
			return refuse(vercelForbidden("Not authorized", nil))
		}
		id.Account = team
	}
	return false
}

// vercelTeamBySlug reads the team a slug names out of the global slug index.
func (s *Server) vercelTeamBySlug(r *http.Request, slug string) (string, bool) {
	raw, ok, err := s.deps.Store.Scope("_mirror", "global").Collection(vercelTeamSlugs).Get(r.Context(), slug)
	if err != nil || !ok {
		return "", false
	}
	var rec struct {
		TeamID string `json:"teamId"`
	}
	if json.Unmarshal(raw, &rec) != nil || rec.TeamID == "" {
		return "", false
	}
	return rec.TeamID, true
}

// vercelAuthFault is Vercel's refusal of a request with no usable credential:
// 403 forbidden, with invalidToken when a token was sent and is not one.
func vercelAuthFault(r *http.Request) *spi.Fault {
	scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	token = strings.TrimSpace(token)
	if !strings.EqualFold(scheme, "Bearer") || token == "" {
		return vercelForbidden("Not authenticated", nil)
	}
	if strings.HasPrefix(strings.ToLower(token), "invalid") {
		return vercelForbidden("Not authorized", map[string]any{"invalidToken": true})
	}
	return nil
}

func vercelForbidden(message string, fields map[string]any) *spi.Fault {
	return &spi.Fault{Code: "forbidden", Message: message, HTTPStatus: http.StatusForbidden, Fault: "client", Fields: fields}
}

// vercelRateLimited is the 429 a client backs off from. The member names and
// the headers are the ones Vercel's clients read (the CLI sleeps for
// Retry-After); the numbers are whatever the control route armed.
func vercelRateLimited(now int64, retryAfter, total int) *spi.Fault {
	reset := now + int64(retryAfter)
	h := http.Header{}
	h.Set("Retry-After", strconv.Itoa(retryAfter))
	h.Set("X-RateLimit-Limit", strconv.Itoa(total))
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
	return &spi.Fault{
		Code:       "rate_limited",
		Message:    "Too many requests - try again in " + strconv.Itoa(retryAfter) + " seconds (more at https://vercel.com/docs/limits#rate-limits)",
		HTTPStatus: http.StatusTooManyRequests,
		Fault:      "client",
		Headers:    h,
		Fields: map[string]any{"limit": map[string]any{
			"remaining": 0, "reset": reset, "resetMs": reset * 1000, "total": total,
		}},
	}
}
