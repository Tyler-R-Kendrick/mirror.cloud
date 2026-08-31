package identity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"net/http"
	"path"
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
	} else if !containsString(strings.Split(signedHeaders, ";"), "x-amz-region-set") {
		return signatureFault()
	}
	canonicalHeaders, ok := signedHeaderValues(r, strings.Split(signedHeaders, ";"))
	if !ok {
		return signatureFault()
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if query && payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	if payloadHash == "" || strings.HasPrefix(payloadHash, "STREAMING-") || !s3PayloadHashMatches(r, payloadHash) {
		return signatureFault()
	}
	canonicalRequest := strings.Join([]string{r.Method, canonicalPath(r.URL), canonicalQuery(r.URL.Query()), canonicalHeaders, signedHeaders, payloadHash}, "\n")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	scope := strings.Join(credential[1:], "/")
	stringToSign := strings.Join([]string{s3V4AAlgorithm, date, scope, hex.EncodeToString(requestHash[:])}, "\n")
	digest := sha256.Sum256([]byte(stringToSign))
	encoded, err := hex.DecodeString(signature)
	key := s3V4AKey(accessKey, secret)
	if err != nil || key == nil || !ecdsa.VerifyASN1(&key.PublicKey, digest[:], encoded) {
		return signatureFault()
	}
	return nil
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
