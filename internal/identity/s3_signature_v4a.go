package identity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const s3V4AAlgorithm = "AWS4-ECDSA-P256-SHA256"

// VerifyS3V4A verifies SigV4A Authorization-header or query authentication.
func VerifyS3V4A(r *http.Request, accessKey, secret, region string) *spi.Fault {
	credential, signedHeaders, signature, date, regionSet, query, ok := s3V4AFields(r)
	signedAt, dateErr := time.Parse("20060102T150405Z", date)
	if !ok || dateErr != nil || len(credential) != 4 || credential[0] != accessKey || credential[1] != signedAt.Format("20060102") || credential[2] != "s3" || credential[3] != "aws4_request" || !s3RegionSetAllows(regionSet, region) {
		return signatureFault()
	}
	if query {
		if containsString(strings.Split(signedHeaders, ";"), "x-amz-region-set") {
			return signatureFault()
		}
	}
	names := strings.Split(signedHeaders, ";")
	canonicalHeaders, ok := signedHeaderValues(r, names)
	if !ok || !s3AmzHeadersSigned(r, names) {
		return signatureFault()
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if query && payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	streaming := payloadHash == "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD" || payloadHash == "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER" || payloadHash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	if payloadHash == "" || strings.HasPrefix(payloadHash, "STREAMING-") && !streaming || !s3PayloadHashMatches(r, payloadHash) {
		return signatureFault()
	}
	canonicalRequest := strings.Join([]string{r.Method, canonicalPath(r.URL), canonicalQuery(r.URL.Query()), canonicalHeaders, signedHeaders, payloadHash}, "\n")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	scope := strings.Join(credential[1:], "/")
	stringToSign := strings.Join([]string{s3V4AAlgorithm, date, scope, hex.EncodeToString(requestHash[:])}, "\n")
	digest := sha256.Sum256([]byte(stringToSign))
	key := s3V4AKey(accessKey, secret)
	if key == nil || !s3V4ASignatureMatches(signature, key, digest[:]) {
		return signatureFault()
	}
	return nil
}

// VerifyS3StreamingV4A verifies chained SigV4A aws-chunked payload and trailer signatures.
func VerifyS3StreamingV4A(r *http.Request, accessKey, secret string, chunks [][]byte, signatures []string, trailers http.Header) *spi.Fault {
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	signedTrailerMode := payloadHash == "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"
	unsignedTrailerMode := payloadHash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	if payloadHash != "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD" && !signedTrailerMode && !unsignedTrailerMode {
		return nil
	}
	credential, signedHeaders, previous, date, _, query, ok := s3V4AFields(r)
	decodedLength, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
	if query || !ok || len(credential) != 4 || credential[0] != accessKey || credential[2] != "s3" || credential[3] != "aws4_request" || !strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") || decodedLength < 0 || err != nil || len(chunks) == 0 || len(chunks) != len(signatures) || len(chunks[len(chunks)-1]) != 0 {
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
	key := s3V4AKey(accessKey, secret)
	if key == nil {
		return signatureFault()
	}
	scope := strings.Join(credential[1:], "/")
	emptyHash := sha256.Sum256(nil)
	emptyHex := hex.EncodeToString(emptyHash[:])
	for i, chunk := range chunks {
		chunkHash := sha256.Sum256(chunk)
		payload := strings.Join([]string{strings.TrimLeft(previous, "*"), emptyHex, hex.EncodeToString(chunkHash[:])}, "\n")
		stringToSign := strings.Join([]string{"AWS4-ECDSA-P256-SHA256-PAYLOAD", date, scope, payload}, "\n")
		digest := sha256.Sum256([]byte(stringToSign))
		if len(signatures[i]) != 144 || !s3V4ASignatureMatches(signatures[i], key, digest[:]) {
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
	payload := strings.TrimLeft(previous, "*") + "\n" + hex.EncodeToString(trailerHash[:])
	stringToSign := strings.Join([]string{"AWS4-ECDSA-P256-SHA256-TRAILER", date, scope, payload}, "\n")
	digest := sha256.Sum256([]byte(stringToSign))
	if signature := trailers.Get("X-Amz-Trailer-Signature"); len(signature) != 144 || !s3V4ASignatureMatches(signature, key, digest[:]) {
		return signatureFault()
	}
	return nil
}

func s3V4ASignatureMatches(signature string, key *ecdsa.PrivateKey, digest []byte) bool {
	encoded, err := hex.DecodeString(strings.TrimLeft(signature, "*"))
	return err == nil && ecdsa.VerifyASN1(&key.PublicKey, digest, encoded)
}

func s3V4AFields(r *http.Request) (credential []string, signedHeaders, signature, date, regionSet string, query, ok bool) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") == s3V4AAlgorithm {
		return strings.Split(q.Get("X-Amz-Credential"), "/"), q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Signature"), q.Get("X-Amz-Date"), q.Get("X-Amz-Region-Set"), true, true
	}
	rest, found := strings.CutPrefix(r.Header.Get("Authorization"), s3V4AAlgorithm+" ")
	if !found {
		return nil, "", "", "", "", false, false
	}
	fields := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		name, value, valid := strings.Cut(strings.TrimSpace(part), "=")
		if !valid || value == "" || fields[name] != "" {
			return nil, "", "", "", "", false, false
		}
		fields[name] = value
	}
	if len(fields) != 3 {
		return nil, "", "", "", "", false, false
	}
	return strings.Split(fields["Credential"], "/"), fields["SignedHeaders"], fields["Signature"], r.Header.Get("X-Amz-Date"), r.Header.Get("X-Amz-Region-Set"), false, true
}

func s3RegionSetAllows(regionSet, region string) bool {
	if regionSet == "" || region == "" {
		return false
	}
	allowed := false
	for _, pattern := range strings.Split(regionSet, ",") {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" || strings.ContainsFunc(pattern, func(r rune) bool { return r != '*' && r != '-' && (r < 'a' || r > 'z') && (r < '0' || r > '9') }) {
			return false
		}
		matched, err := path.Match(pattern, region)
		if err != nil {
			return false
		}
		allowed = allowed || matched
	}
	return allowed
}

func s3V4AKey(accessKey, secret string) *ecdsa.PrivateKey {
	curve := elliptic.P256()
	maximum := new(big.Int).Sub(curve.Params().N, big.NewInt(2))
	for counter := byte(1); counter < 255; counter++ {
		fixed := []byte{0, 0, 0, 1}
		fixed = append(fixed, s3V4AAlgorithm...)
		fixed = append(fixed, 0)
		fixed = append(fixed, accessKey...)
		fixed = append(fixed, counter, 0, 0, 1, 0)
		candidate := new(big.Int).SetBytes(hmacSHA256([]byte("AWS4A"+secret), string(fixed)))
		if candidate.Cmp(maximum) > 0 {
			continue
		}
		candidate.Add(candidate, big.NewInt(1))
		x, y := curve.ScalarBaseMult(candidate.Bytes())
		return &ecdsa.PrivateKey{PublicKey: ecdsa.PublicKey{Curve: curve, X: x, Y: y}, D: candidate}
	}
	return nil
}
