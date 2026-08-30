// Package identity parses SigV4 credentials and never verifies signatures.
package identity

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const defaultAccount = "000000000000"
const defaultRegion = "us-east-1"

// Parse extracts Identity from r. Signatures are not verified.
// now is the process clock; used only for presigned-URL expiry.
func Parse(r *http.Request, defaultAcct, defaultReg string, now time.Time) spi.Identity {
	if defaultAcct == "" {
		defaultAcct = defaultAccount
	}
	if defaultReg == "" {
		defaultReg = defaultRegion
	}
	id := spi.Identity{Account: defaultAcct, Region: defaultReg, ARN: arn(defaultAcct)}

	if v := r.Header.Get("X-Mirror-Region"); v != "" {
		id.Region = v
	}
	if v := r.Header.Get("X-Mirror-Account-Id"); len(v) == 12 {
		id.Account = v
		id.ARN = arn(v)
	}

	cred := r.Header.Get("Authorization")
	if cred == "" {
		cred = r.URL.Query().Get("X-Amz-Credential")
		if expires, ok := PresignedExpiry(r); ok && !now.UTC().Before(expires) {
			// expiry is enforced; identity still returned so the edge can
			// return the service's modeled fault. Callers check Expired.
			id.ARN += ":expired"
		}
	}
	expired := strings.HasSuffix(id.ARN, ":expired")
	akid, region := parseCredential(cred)
	if akid != "" {
		id.AccessKeyID = akid
		if r.Header.Get("X-Mirror-Region") == "" && region != "" {
			id.Region = region
		}
		if r.Header.Get("X-Mirror-Account-Id") == "" {
			if len(akid) == 12 && isDigits(akid) {
				id.Account = akid
			}
			id.ARN = arn(id.Account)
		}
	}
	if expired && !strings.HasSuffix(id.ARN, ":expired") {
		id.ARN += ":expired"
	}
	if id.Project == "" {
		id.Project = id.Account
	}
	if v := r.Header.Get("X-Mirror-Role"); v != "" {
		id.ARN = "arn:aws:sts::" + id.Account + ":assumed-role/" + v + "/mirror"
	}
	return id
}

// PresignedExpiry returns the expiry instant for SigV2 or SigV4 query authentication.
func PresignedExpiry(r *http.Request) (time.Time, bool) {
	q := r.URL.Query()
	if raw := q.Get("X-Amz-Expires"); raw != "" {
		secs, err := strconv.ParseInt(raw, 10, 64)
		started, dateErr := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
		if err == nil && dateErr == nil && secs >= 0 {
			return started.Add(time.Duration(secs) * time.Second), true
		}
	}
	if raw := q.Get("Expires"); raw != "" && q.Get("AWSAccessKeyId") != "" {
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err == nil {
			return time.Unix(seconds, 0).UTC(), true
		}
	}
	return time.Time{}, false
}

func parseCredential(h string) (akid, region string) {
	// Authorization: AWS4-HMAC-SHA256 Credential=<AKID>/<date>/<region>/<service>/aws4_request, ...
	// or raw query credential.
	idx := strings.Index(h, "Credential=")
	rest := h
	if idx >= 0 {
		rest = h[idx+len("Credential="):]
	}
	rest = strings.Split(rest, ",")[0]
	rest = strings.TrimSpace(rest)
	parts := strings.Split(rest, "/")
	if len(parts) >= 1 {
		akid = parts[0]
	}
	if len(parts) >= 3 {
		region = parts[2]
	}
	return akid, region
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func arn(account string) string {
	return "arn:aws:iam::" + account + ":user/mirror-local"
}

// Expired reports whether Parse marked the identity as an expired presign.
func Expired(id spi.Identity) bool {
	return strings.HasSuffix(id.ARN, ":expired")
}
