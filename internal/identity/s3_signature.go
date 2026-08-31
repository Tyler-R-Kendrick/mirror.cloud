package identity

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// VerifyS3Presigned verifies supported query-signature versions when present.
func VerifyS3Presigned(r *http.Request, secret string) *spi.Fault {
	if fault := VerifyS3PresignedV4(r, secret); fault != nil {
		return fault
	}
	return VerifyS3PresignedV2(r, secret)
}

// VerifyS3SessionToken verifies the temporary credential token on a presigned request.
func VerifyS3SessionToken(r *http.Request, expected string) *spi.Fault {
	token := r.URL.Query().Get("X-Amz-Security-Token")
	if token == "" {
		token = r.Header.Get("X-Amz-Security-Token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		return &spi.Fault{Code: "InvalidToken", Message: "The provided token is malformed or otherwise invalid.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	return nil
}

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

// VerifyS3PresignedV2 verifies SigV2 query authentication when present.
func VerifyS3PresignedV2(r *http.Request, secret string) *spi.Fault {
	q := r.URL.Query()
	if q.Get("AWSAccessKeyId") == "" {
		return nil
	}
	stringToSign := strings.Join([]string{r.Method, r.Header.Get("Content-MD5"), r.Header.Get("Content-Type"), q.Get("Expires"), canonicalV2AmzHeaders(r) + canonicalV2Resource(r)}, "\n")
	want := hmacSHA1([]byte(secret), stringToSign)
	got, err := base64.StdEncoding.DecodeString(q.Get("Signature"))
	if err != nil {
		return signatureFault()
	}
	if len(got) != len(want) || subtle.ConstantTimeCompare(got, want) != 1 {
		return signatureFault()
	}
	return nil
}

func canonicalV2AmzHeaders(r *http.Request) string {
	values := map[string][]string{}
	for name, entries := range r.Header {
		name = strings.ToLower(name)
		if strings.HasPrefix(name, "x-amz-") {
			values[name] = append(values[name], entries...)
		}
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(':')
		for i, value := range values[name] {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strings.Join(strings.Fields(value), " "))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func canonicalV2Resource(r *http.Request) string {
	path := canonicalPath(r.URL)
	host := strings.ToLower(r.Host)
	if colon := strings.LastIndexByte(host, ':'); colon >= 0 {
		host = host[:colon]
	}
	if marker := strings.Index(host, ".s3."); marker > 0 {
		path = "/" + host[:marker] + path
	} else if marker := strings.Index(host, ".s3-"); marker > 0 {
		path = "/" + host[:marker] + path
	} else if strings.HasSuffix(host, ".s3.amazonaws.com") {
		path = "/" + strings.TrimSuffix(host, ".s3.amazonaws.com") + path
	}
	signed := map[string]bool{"acl": true, "delete": true, "lifecycle": true, "location": true, "logging": true, "notification": true, "partNumber": true, "policy": true, "requestPayment": true, "response-cache-control": true, "response-content-disposition": true, "response-content-encoding": true, "response-content-language": true, "response-content-type": true, "response-expires": true, "uploadId": true, "uploads": true, "versionId": true, "versioning": true, "versions": true, "website": true}
	var query []string
	for name, entries := range r.URL.Query() {
		if !signed[name] {
			continue
		}
		if len(entries) == 0 || entries[0] == "" {
			query = append(query, name)
		} else {
			for _, value := range entries {
				query = append(query, name+"="+value)
			}
		}
	}
	sort.Strings(query)
	if len(query) > 0 {
		path += "?" + strings.Join(query, "&")
	}
	return path
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

func hmacSHA1(key []byte, value string) []byte {
	h := hmac.New(sha1.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func signatureFault() *spi.Fault {
	return &spi.Fault{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", HTTPStatus: http.StatusForbidden, Fault: "client"}
}
