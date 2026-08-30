package identity

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// VerifyS3Signature verifies supported S3 signatures when present.
func VerifyS3Signature(r *http.Request, secret string) *spi.Fault {
	if fault := VerifyS3Presigned(r, secret); fault != nil {
		return fault
	}
	return VerifyS3AuthorizationV4(r, secret)
}

// VerifyS3Presigned verifies supported query-signature versions when present.
func VerifyS3Presigned(r *http.Request, secret string) *spi.Fault {
	if fault := VerifyS3PresignedV4(r, secret); fault != nil {
		return fault
	}
	return VerifyS3PresignedV2(r, secret)
}

// VerifyS3AuthorizationV4 verifies SigV4 Authorization-header authentication when present.
func VerifyS3AuthorizationV4(r *http.Request, secret string) *spi.Fault {
	credential, signedHeaders, signature, present, ok := s3AuthorizationV4(r)
	if !present {
		return nil
	}
	if !ok {
		return signatureFault()
	}
	canonicalHeaders, ok := signedHeaderValues(r, strings.Split(signedHeaders, ";"))
	if !ok {
		return signatureFault()
	}
	date := r.Header.Get("X-Amz-Date")
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if date == "" || payloadHash == "" {
		return signatureFault()
	}
	if !s3PayloadHashMatches(r, payloadHash) {
		return signatureFault()
	}
	canonicalRequest := strings.Join([]string{r.Method, canonicalPath(r.URL), canonicalQuery(r.URL.Query()), canonicalHeaders, signedHeaders, payloadHash}, "\n")
	return verifyS3V4Signature(credential, date, canonicalRequest, signature, secret)
}

// VerifyS3StreamingV4 verifies the chained signatures of a signed aws-chunked payload.
func VerifyS3StreamingV4(r *http.Request, secret string, chunks [][]byte, signatures []string) *spi.Fault {
	if r.Header.Get("X-Amz-Content-Sha256") != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		return nil
	}
	credential, _, previous, present, ok := s3AuthorizationV4(r)
	if !present || !ok || len(chunks) == 0 || len(chunks) != len(signatures) || len(chunks[len(chunks)-1]) != 0 {
		return signatureFault()
	}
	date := r.Header.Get("X-Amz-Date")
	scope := strings.Join(credential[1:], "/")
	signingKey := s3V4SigningKey(credential, secret)
	emptyHash := sha256.Sum256(nil)
	emptyHex := hex.EncodeToString(emptyHash[:])
	for i, chunk := range chunks {
		chunkHash := sha256.Sum256(chunk)
		stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256-PAYLOAD", date, scope, previous, emptyHex, hex.EncodeToString(chunkHash[:])}, "\n")
		want := hmacSHA256(signingKey, stringToSign)
		if !s3V4SignatureMatches(signatures[i], want) {
			return signatureFault()
		}
		previous = signatures[i]
	}
	return nil
}

func s3AuthorizationV4(r *http.Request) (credential []string, signedHeaders, signature string, present, ok bool) {
	const algorithm = "AWS4-HMAC-SHA256"
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, algorithm) {
		return nil, "", "", false, true
	}
	present = true
	rest, valid := strings.CutPrefix(authorization, algorithm+" ")
	if !valid {
		return nil, "", "", true, false
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		name, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found || value == "" || fields[name] != "" {
			return nil, "", "", true, false
		}
		fields[name] = value
	}
	if len(fields) != 3 || fields["Credential"] == "" || fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return nil, "", "", true, false
	}
	credential = strings.Split(fields["Credential"], "/")
	if len(credential) != 5 || credential[3] != "s3" || credential[4] != "aws4_request" {
		return nil, "", "", true, false
	}
	return credential, fields["SignedHeaders"], fields["Signature"], true, true
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
	if len(credential) != 5 || credential[3] != "s3" || credential[4] != "aws4_request" || q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
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
	if !s3PayloadHashMatches(r, payloadHash) {
		return signatureFault()
	}
	canonicalRequest := strings.Join([]string{r.Method, canonicalPath(r.URL), canonicalQuery(q), canonicalHeaders, signedHeaders, payloadHash}, "\n")
	return verifyS3V4Signature(credential, q.Get("X-Amz-Date"), canonicalRequest, q.Get("X-Amz-Signature"), secret)
}

func verifyS3V4Signature(credential []string, date, canonicalRequest, signature, secret string) *spi.Fault {
	scope := strings.Join(credential[1:], "/")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", date, scope, hex.EncodeToString(requestHash[:])}, "\n")
	signingKey := s3V4SigningKey(credential, secret)
	want := hmacSHA256(signingKey, stringToSign)
	if !s3V4SignatureMatches(signature, want) {
		return signatureFault()
	}
	return nil
}

func s3V4SignatureMatches(signature string, want []byte) bool {
	got, err := hex.DecodeString(signature)
	return err == nil && len(got) == len(want) && subtle.ConstantTimeCompare(got, want) == 1
}

func s3V4SigningKey(credential []string, secret string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), credential[1])
	regionKey := hmacSHA256(dateKey, credential[2])
	serviceKey := hmacSHA256(regionKey, credential[3])
	return hmacSHA256(serviceKey, "aws4_request")
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
		} else if name == "content-length" && r.ContentLength >= 0 {
			value = strconv.FormatInt(r.ContentLength, 10)
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

func s3PayloadHashMatches(r *http.Request, payloadHash string) bool {
	if payloadHash == "UNSIGNED-PAYLOAD" || strings.HasPrefix(payloadHash, "STREAMING-") {
		return true
	}
	want, err := hex.DecodeString(payloadHash)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	var body []byte
	if r.Body != nil {
		body, err = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	got := sha256.Sum256(body)
	return err == nil && subtle.ConstantTimeCompare(got[:], want) == 1
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
