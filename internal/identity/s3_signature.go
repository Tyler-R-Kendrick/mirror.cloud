package identity

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// VerifyS3PresignedV4 verifies SigV4 query authentication when present.
func VerifyS3PresignedV4(r *http.Request, secret string) *spi.Fault {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") == "" {
		return nil
	}
	credential := strings.Split(q.Get("X-Amz-Credential"), "/")
	if len(credential) != 5 || credential[4] != "aws4_request" || q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		return signatureFault()
	}
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	canonicalHeaders, ok := signedHeaderValues(r, strings.Split(signedHeaders, ";"))
	if !ok {
		return signatureFault()
	}
	payloadHash := q.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = r.Header.Get("X-Amz-Content-Sha256")
	}
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	canonicalRequest := strings.Join([]string{r.Method, canonicalPath(r.URL), canonicalQuery(q), canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := strings.Join(credential[1:], "/")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", q.Get("X-Amz-Date"), scope, hex.EncodeToString(requestHash[:])}, "\n")
	dateKey := hmacSHA256([]byte("AWS4"+secret), credential[1])
	regionKey := hmacSHA256(dateKey, credential[2])
	serviceKey := hmacSHA256(regionKey, credential[3])
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	want := hmacSHA256(signingKey, stringToSign)
	got, err := hex.DecodeString(q.Get("X-Amz-Signature"))
	if err != nil || len(got) != len(want) || subtle.ConstantTimeCompare(got, want) != 1 {
		return signatureFault()
	}
	return nil
}

func signedHeaderValues(r *http.Request, names []string) (string, bool) {
	var b strings.Builder
	for _, name := range names {
		if name == "" || name != strings.ToLower(name) {
			return "", false
		}
		value := strings.Join(r.Header.Values(name), ",")
		if name == "host" {
			value = r.Host
		}
		if value == "" {
			return "", false
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.Join(strings.Fields(value), " "))
		b.WriteByte('\n')
	}
	return b.String(), true
}

func canonicalPath(u *url.URL) string {
	if path := u.EscapedPath(); path != "" {
		return path
	}
	return "/"
}

func canonicalQuery(q url.Values) string {
	var values []string
	for key, entries := range q {
		if key == "X-Amz-Signature" {
			continue
		}
		for _, value := range entries {
			values = append(values, awsEscape(key)+"="+awsEscape(value))
		}
	}
	sort.Strings(values)
	return strings.Join(values, "&")
}

func awsEscape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func signatureFault() *spi.Fault {
	return &spi.Fault{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", HTTPStatus: http.StatusForbidden, Fault: "client"}
}
