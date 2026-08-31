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
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// VerifyS3Signature verifies supported S3 signatures when present.
func VerifyS3Signature(r *http.Request, accessKey, secret, region string) *spi.Fault {
	if strings.HasPrefix(r.Header.Get("Authorization"), s3V4AAlgorithm) || r.URL.Query().Get("X-Amz-Algorithm") == s3V4AAlgorithm {
		return VerifyS3V4A(r, accessKey, secret, region)
	}
	if fault := VerifyS3Presigned(r, secret); fault != nil {
		return fault
	}
	if fault := VerifyS3AuthorizationV4(r, secret); fault != nil {
		return fault
	}
	return VerifyS3AuthorizationV2(r, secret)
}

// VerifyS3Presigned verifies supported query-signature versions when present.
func VerifyS3Presigned(r *http.Request, secret string) *spi.Fault {
	if fault := VerifyS3PresignedV4(r, secret); fault != nil {
		return fault
	}
	return VerifyS3PresignedV2(r, secret)
}

// VerifyS3PostPolicy verifies supported browser-upload policy signatures when present.
func VerifyS3PostPolicy(fields map[string]string, secret string) *spi.Fault {
	form := make(map[string]string, len(fields))
	for key, value := range fields {
		form[strings.ToLower(key)] = value
	}
	policy := form["policy"]
	if policy == "" {
		return nil
	}
	if form["x-amz-signature"] != "" {
		credential := strings.Split(form["x-amz-credential"], "/")
		date, err := time.Parse("20060102T150405Z", form["x-amz-date"])
		if form["x-amz-algorithm"] != "AWS4-HMAC-SHA256" || err != nil || len(credential) != 5 || credential[1] != date.Format("20060102") || credential[3] != "s3" || credential[4] != "aws4_request" {
			return signatureFault()
		}
		if !s3V4SignatureMatches(form["x-amz-signature"], hmacSHA256(s3V4SigningKey(credential, secret), policy)) {
			return signatureFault()
		}
		return nil
	}
	if form["signature"] != "" {
		return verifyS3V2Signature(form["signature"], secret, policy)
	}
	return nil
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

// VerifyS3AuthorizationV2 verifies SigV2 Authorization-header authentication when present.
func VerifyS3AuthorizationV2(r *http.Request, secret string) *spi.Fault {
	authorization, present := strings.CutPrefix(r.Header.Get("Authorization"), "AWS ")
	if !present {
		return nil
	}
	accessKey, signature, ok := strings.Cut(authorization, ":")
	if !ok || accessKey == "" || signature == "" || strings.Contains(signature, ":") {
		return signatureFault()
	}
	date := r.Header.Get("Date")
	if r.Header.Get("X-Amz-Date") != "" {
		date = ""
	} else if date == "" {
		return signatureFault()
	}
	stringToSign := strings.Join([]string{r.Method, r.Header.Get("Content-MD5"), r.Header.Get("Content-Type"), date, canonicalV2AmzHeaders(r) + canonicalV2Resource(r)}, "\n")
	return verifyS3V2Signature(signature, secret, stringToSign)
}

// VerifyS3StreamingV4 verifies the chained signatures of a signed aws-chunked payload and trailers.
func VerifyS3StreamingV4(r *http.Request, secret string, chunks [][]byte, signatures []string, trailers http.Header) *spi.Fault {
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	signedTrailerMode := payloadHash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	unsignedTrailerMode := payloadHash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	if payloadHash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" && !signedTrailerMode && !unsignedTrailerMode {
		return nil
	}
	credential, signedHeaders, previous, present, ok := s3AuthorizationV4(r)
	decodedLength, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if !present || !ok || !strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") || decodedLength < 0 || err != nil || len(chunks) == 0 || len(chunks) != len(signatures) || len(chunks[len(chunks)-1]) != 0 {
		return signatureFault()
	}
	if (signedTrailerMode || unsignedTrailerMode) && !containsString(strings.Split(signedHeaders, ";"), "x-amz-trailer") {
		return signatureFault()
	}
	var actualLength int64
	for _, chunk := range chunks {
		actualLength += int64(len(chunk))
	}
	if actualLength != decodedLength {
		return signatureFault()
	}
	if unsignedTrailerMode {
		for _, signature := range signatures {
			if signature != "" {
				return signatureFault()
			}
		}
		if _, ok := canonicalS3StreamingTrailers(r, trailers, false); !ok {
			return signatureFault()
		}
		return nil
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
	if !signedTrailerMode {
		if len(trailers) != 0 {
			return signatureFault()
		}
		return nil
	}
	canonical, ok := canonicalS3StreamingTrailers(r, trailers, true)
	if !ok {
		return signatureFault()
	}
	trailerHash := sha256.Sum256([]byte(canonical))
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256-TRAILER", date, scope, previous, hex.EncodeToString(trailerHash[:])}, "\n")
	if !s3V4SignatureMatches(trailers.Get("X-Amz-Trailer-Signature"), hmacSHA256(signingKey, stringToSign)) {
		return signatureFault()
	}
	return nil
}

func canonicalS3StreamingTrailers(r *http.Request, trailers http.Header, signed bool) (string, bool) {
	declared := strings.Split(strings.ToLower(r.Header.Get("X-Amz-Trailer")), ",")
	for i := range declared {
		declared[i] = strings.TrimSpace(declared[i])
		if declared[i] == "" {
			return "", false
		}
	}
	sort.Strings(declared)
	want := len(declared)
	if signed {
		want++
	}
	if len(trailers) != want || signed != (len(trailers.Values("X-Amz-Trailer-Signature")) == 1) {
		return "", false
	}
	var canonical strings.Builder
	for i, name := range declared {
		if i > 0 && name == declared[i-1] {
			return "", false
		}
		values := trailers.Values(name)
		if len(values) != 1 {
			return "", false
		}
		canonical.WriteString(name)
		canonical.WriteByte(':')
		canonical.WriteString(strings.Join(strings.Fields(values[0]), " "))
		canonical.WriteByte('\n')
	}
	return canonical.String(), true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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
	return VerifyS3SessionTokenValue(token, expected)
}

// VerifyS3SessionTokenValue verifies a temporary credential token supplied outside headers or query parameters.
func VerifyS3SessionTokenValue(token, expected string) *spi.Fault {
	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		return &spi.Fault{Code: "InvalidToken", Message: "The provided token is malformed or otherwise invalid.", HTTPStatus: http.StatusBadRequest, Fault: "client"}
	}
	return nil
}

// S3AuthorizationTimeFault enforces S3's 15-minute clock-skew window for header authentication.
func S3AuthorizationTimeFault(r *http.Request, now time.Time) *spi.Fault {
	authorization := r.Header.Get("Authorization")
	var requestTime time.Time
	var err error
	switch {
	case strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 "), strings.HasPrefix(authorization, s3V4AAlgorithm+" "):
		requestTime, err = time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
	case strings.HasPrefix(authorization, "AWS "):
		date := r.Header.Get("X-Amz-Date")
		if date == "" {
			date = r.Header.Get("Date")
		}
		requestTime, err = http.ParseTime(date)
	default:
		return nil
	}
	if err != nil {
		return signatureFault()
	}
	now = now.UTC()
	if !requestTime.Before(now.Add(-15*time.Minute)) && !requestTime.After(now.Add(15*time.Minute)) {
		return nil
	}
	return &spi.Fault{
		Code:       "RequestTimeTooSkewed",
		Message:    "The difference between the request time and the server's time is too large.",
		HTTPStatus: http.StatusForbidden,
		Fault:      "client",
		Fields: map[string]any{
			"RequestTime": requestTime.UTC().Format(time.RFC3339),
			"ServerTime":  now.Format(time.RFC3339),
		},
	}
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
	return verifyS3V2Signature(q.Get("Signature"), secret, stringToSign)
}

func verifyS3V2Signature(signature, secret, stringToSign string) *spi.Fault {
	want := hmacSHA1([]byte(secret), stringToSign)
	got, err := base64.StdEncoding.DecodeString(signature)
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
