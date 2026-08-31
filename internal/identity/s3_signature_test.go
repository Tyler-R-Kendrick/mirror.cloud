package identity

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVerifyS3PresignedV4AWSExample(t *testing.T) {
	const rawURL = "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	request := httptest.NewRequest(http.MethodGet, rawURL, nil)
	if fault := VerifyS3PresignedV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault != nil {
		t.Fatalf("official AWS example rejected: %#v", fault)
	}
	query := request.URL.Query()
	query.Set("X-Amz-Signature", "00eed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404")
	request.URL.RawQuery = query.Encode()
	if fault := VerifyS3PresignedV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signature accepted: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV4AWSExample(t *testing.T) {
	request := awsAuthorizationExample()
	if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault != nil {
		t.Fatalf("official AWS example rejected: %#v", fault)
	}
	request.Header.Set("Range", "bytes=1-9")
	if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed header accepted: %#v", fault)
	}
	request = awsAuthorizationExample()
	request.Body = io.NopCloser(strings.NewReader("tampered"))
	if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered payload accepted: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV4RejectsMalformed(t *testing.T) {
	for name, mutate := range map[string]func(*http.Request){
		"duplicate field": func(r *http.Request) { r.Header.Set("Authorization", r.Header.Get("Authorization")+",Signature=00") },
		"missing date": func(r *http.Request) {
			r.Header.Del("X-Amz-Date")
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256,Signature=29575ca434952f9bf8b87b84161cfacd0a7809cc36a3c12984abaef2bdc7e4f6")
		},
		"missing payload": func(r *http.Request) {
			r.Header.Del("X-Amz-Content-Sha256")
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=1b0e851a1c8d0a8c713d10578e54ae3d418b1d457acf62c7a22dd66c3b50f178")
		},
		"missing header": func(r *http.Request) { r.Header.Del("Range") },
		"wrong service": func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "/s3/", "/sts/", 1))
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", "138618539e0e3b441c624f9b403b36cd3c81c715ee31d553b9c17446d453bf4c", 1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := awsAuthorizationExample()
			mutate(request)
			if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("malformed authorization accepted: %#v", fault)
			}
		})
	}
	request := awsAuthorizationExample()
	request.Header.Set("Authorization", "Bearer token")
	if fault := VerifyS3AuthorizationV4(request, "unused"); fault != nil {
		t.Fatalf("unrelated authorization rejected: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV2AWSExample(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set("Date", "Tue, 27 Mar 2007 19:36:42 +0000")
	request.Header.Set("Authorization", "AWS AKIAIOSFODNN7EXAMPLE:qgk2+6Sv9/oM7G3qLEjTH1a1l1g=")
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if fault := VerifyS3AuthorizationV2(request, secret); fault != nil {
		t.Fatalf("official AWS example rejected: %#v", fault)
	}
	request.Header.Set("Date", "Tue, 27 Mar 2007 19:36:43 +0000")
	if fault := VerifyS3AuthorizationV2(request, secret); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered date accepted: %#v", fault)
	}
	if fault := VerifyS3Signature(request, "AKIAIOSFODNN7EXAMPLE", secret, "us-west-1"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("dispatcher accepted tampered SigV2 authorization: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV2DateAndGrammar(t *testing.T) {
	request := httptest.NewRequest(http.MethodDelete, "https://localhost/bucket/key", nil)
	request.Header.Set("Date", "ignored")
	request.Header.Set("X-Amz-Date", "Tue, 27 Mar 2007 21:20:26 +0000")
	stringToSign := "DELETE\n\n\n\nx-amz-date:Tue, 27 Mar 2007 21:20:26 +0000\n/bucket/key"
	signature := base64.StdEncoding.EncodeToString(hmacSHA1([]byte("test"), stringToSign))
	request.Header.Set("Authorization", "AWS test:"+signature)
	if fault := VerifyS3AuthorizationV2(request, "test"); fault != nil {
		t.Fatalf("x-amz-date request rejected: %#v", fault)
	}
	request.Header.Set("Date", "still ignored")
	if fault := VerifyS3AuthorizationV2(request, "test"); fault != nil {
		t.Fatalf("Date overrode x-amz-date: %#v", fault)
	}
	for _, authorization := range []string{"AWS ", "AWS test", "AWS :" + signature, "AWS test:", "AWS test:" + signature + ":extra"} {
		request.Header.Set("Authorization", authorization)
		if fault := VerifyS3AuthorizationV2(request, "test"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
			t.Fatalf("malformed %q accepted: %#v", authorization, fault)
		}
	}
	request.Header.Del("Date")
	request.Header.Del("X-Amz-Date")
	emptyDateSignature := base64.StdEncoding.EncodeToString(hmacSHA1([]byte("test"), "DELETE\n\n\n\n/bucket/key"))
	request.Header.Set("Authorization", "AWS test:"+emptyDateSignature)
	if fault := VerifyS3AuthorizationV2(request, "test"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("missing timestamp accepted: %#v", fault)
	}
	request.Header.Set("Authorization", "Bearer token")
	if fault := VerifyS3AuthorizationV2(request, "test"); fault != nil {
		t.Fatalf("unrelated authorization rejected: %#v", fault)
	}
}

func TestVerifyS3PostPolicy(t *testing.T) {
	policy := base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T00:00:00Z","conditions":[]}`))
	credential := []string{"test", "20990101", "us-east-1", "s3", "aws4_request"}
	v4 := map[string]string{
		"policy":           policy,
		"x-amz-algorithm":  "AWS4-HMAC-SHA256",
		"x-amz-credential": strings.Join(credential, "/"),
		"x-amz-date":       "20990101T000000Z",
		"x-amz-signature":  hex.EncodeToString(hmacSHA256(s3V4SigningKey(credential, "test"), policy)),
	}
	if fault := VerifyS3PostPolicy(v4, "test"); fault != nil {
		t.Fatalf("valid SigV4 policy rejected: %#v", fault)
	}
	for name, value := range map[string]string{"signature": "00", "algorithm": "AWS4-ECDSA-P256-SHA256", "date": "20990102T000000Z"} {
		t.Run(name, func(t *testing.T) {
			fields := make(map[string]string, len(v4))
			for key, original := range v4 {
				fields[key] = original
			}
			switch name {
			case "signature":
				fields["x-amz-signature"] = value
			case "algorithm":
				fields["x-amz-algorithm"] = value
			case "date":
				fields["x-amz-date"] = value
			}
			if fault := VerifyS3PostPolicy(fields, "test"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("tampered policy accepted: %#v", fault)
			}
		})
	}
	v2 := map[string]string{"policy": policy, "AWSAccessKeyId": "test", "signature": base64.StdEncoding.EncodeToString(hmacSHA1([]byte("test"), policy))}
	if fault := VerifyS3PostPolicy(v2, "test"); fault != nil {
		t.Fatalf("valid SigV2 policy rejected: %#v", fault)
	}
	v2["signature"] = "tampered"
	if fault := VerifyS3PostPolicy(v2, "test"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered SigV2 policy accepted: %#v", fault)
	}
}

func TestS3V4AKeyAWSVector(t *testing.T) {
	key := s3V4AKey("AKISORANDOMAASORANDOM", "q+jcrXGc+0zWN6uzclKVhvMmUsIfRPa4rlRandom")
	if key == nil || key.D.Text(16) != "7fd3bd010c0d9c292141c2b77bfbde1042c92e6836fff749d1269ec890fca1bd" || key.X.Text(16) != "15d242ceebf8d8169fd6a8b5a746c41140414c3b07579038da06af89190fffcb" || key.Y.Text(16) != "515242cedd82e94799482e4c0514b505afccf2c0c98d6a553bf539f424c5ec0" {
		t.Fatalf("AWS SigV4A key vector mismatch: %#v", key)
	}
}

func TestVerifyS3V4A(t *testing.T) {
	const accessKey = "AKISORANDOMAASORANDOM"
	const secret = "q+jcrXGc+0zWN6uzclKVhvMmUsIfRPa4rlRandom"
	const date = "20990101T000000Z"
	sign := func(t *testing.T, request *http.Request, credential []string, signedHeaders, payloadHash string) string {
		t.Helper()
		canonicalHeaders, ok := signedHeaderValues(request, strings.Split(signedHeaders, ";"))
		if !ok {
			t.Fatal("invalid test headers")
		}
		canonical := strings.Join([]string{request.Method, canonicalPath(request.URL), canonicalQuery(request.URL.Query()), canonicalHeaders, signedHeaders, payloadHash}, "\n")
		requestHash := sha256.Sum256([]byte(canonical))
		stringToSign := strings.Join([]string{s3V4AAlgorithm, date, strings.Join(credential[1:], "/"), hex.EncodeToString(requestHash[:])}, "\n")
		digest := sha256.Sum256([]byte(stringToSign))
		signature, err := ecdsa.SignASN1(bytes.NewReader(bytes.Repeat([]byte{0x42}, 1024)), s3V4AKey(accessKey, secret), digest[:])
		if err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(signature)
	}
	credential := []string{accessKey, "20990101", "s3", "aws4_request"}
	signedHeaders := "host;x-amz-content-sha256;x-amz-date;x-amz-region-set"
	header := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	header.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	header.Header.Set("X-Amz-Date", date)
	header.Header.Set("X-Amz-Region-Set", "us-east-1,us-west-*")
	header.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(credential, "/")+",SignedHeaders="+signedHeaders+",Signature="+sign(t, header, credential, signedHeaders, "UNSIGNED-PAYLOAD"))
	if fault := VerifyS3Signature(header, accessKey, secret, "us-west-2"); fault != nil {
		t.Fatalf("valid SigV4A header rejected: %#v", fault)
	}
	if fault := VerifyS3V4A(header, "wrong", secret, "us-west-2"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("wrong access key accepted: %#v", fault)
	}
	wrongCredential := append([]string(nil), credential...)
	wrongCredential[0] = "wrong"
	header.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(wrongCredential, "/")+",SignedHeaders="+signedHeaders+",Signature="+sign(t, header, wrongCredential, signedHeaders, "UNSIGNED-PAYLOAD"))
	if fault := VerifyS3V4A(header, accessKey, secret, "us-west-2"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("mismatched credential access key accepted: %#v", fault)
	}
	header.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(credential, "/")+",SignedHeaders="+signedHeaders+",Signature="+sign(t, header, credential, signedHeaders, "UNSIGNED-PAYLOAD"))
	unsignedRegion := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	unsignedRegion.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	unsignedRegion.Header.Set("X-Amz-Date", date)
	unsignedRegion.Header.Set("X-Amz-Region-Set", "us-east-1,us-west-*")
	unsignedRegionHeaders := "host;x-amz-content-sha256;x-amz-date"
	unsignedRegion.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(credential, "/")+",SignedHeaders="+unsignedRegionHeaders+",Signature="+sign(t, unsignedRegion, credential, unsignedRegionHeaders, "UNSIGNED-PAYLOAD"))
	if fault := VerifyS3V4A(unsignedRegion, accessKey, secret, "us-west-2"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("unsigned region set accepted: %#v", fault)
	}
	malformed := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	malformed.Header.Set("Authorization", s3V4AAlgorithm+"broken")
	if fault := VerifyS3Signature(malformed, accessKey, secret, "us-east-1"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("malformed SigV4A header accepted: %#v", fault)
	}
	if fault := VerifyS3V4A(header, accessKey, secret, "eu-west-1"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("out-of-set region accepted: %#v", fault)
	}
	streamingHash := "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
	header.Header.Set("X-Amz-Content-Sha256", streamingHash)
	header.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(credential, "/")+",SignedHeaders="+signedHeaders+",Signature="+sign(t, header, credential, signedHeaders, streamingHash))
	if fault := VerifyS3V4A(header, accessKey, secret, "us-west-2"); fault != nil {
		t.Fatalf("supported SigV4A stream seed rejected: %#v", fault)
	}
	unknownStreamingHash := "STREAMING-AWS4-ECDSA-P256-SHA256-UNKNOWN"
	header.Header.Set("X-Amz-Content-Sha256", unknownStreamingHash)
	header.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(credential, "/")+",SignedHeaders="+signedHeaders+",Signature="+sign(t, header, credential, signedHeaders, unknownStreamingHash))
	if fault := VerifyS3V4A(header, accessKey, secret, "us-west-2"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("unknown SigV4A stream mode accepted: %#v", fault)
	}

	query := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	values := query.URL.Query()
	values.Set("X-Amz-Algorithm", s3V4AAlgorithm)
	values.Set("X-Amz-Credential", strings.Join(credential, "/"))
	values.Set("X-Amz-Date", date)
	values.Set("X-Amz-Expires", "60")
	values.Set("X-Amz-Region-Set", "us-east-1,us-west-*")
	values.Set("X-Amz-SignedHeaders", "host")
	query.URL.RawQuery = values.Encode()
	values.Set("X-Amz-Signature", sign(t, query, credential, "host", "UNSIGNED-PAYLOAD"))
	query.URL.RawQuery = values.Encode()
	if fault := VerifyS3Signature(query, accessKey, secret, "us-east-1"); fault != nil {
		t.Fatalf("valid SigV4A query rejected: %#v", fault)
	}
	values.Set("X-Amz-Signature", "00")
	query.URL.RawQuery = values.Encode()
	if fault := VerifyS3V4A(query, accessKey, secret, "us-east-1"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("bad SigV4A signature accepted: %#v", fault)
	}
	values.Set("X-Amz-Signature", sign(t, query, credential, "host", "UNSIGNED-PAYLOAD"))
	values.Set("X-Amz-Region-Set", "eu-*")
	query.URL.RawQuery = values.Encode()
	if fault := VerifyS3V4A(query, accessKey, secret, "us-east-1"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered query region set accepted: %#v", fault)
	}
}

func TestS3AuthorizationTimeFault(t *testing.T) {
	now := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	request := httptest.NewRequest(http.MethodGet, "https://example.test", nil)
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 signed")
	for name, at := range map[string]time.Time{
		"past boundary":   now.Add(-15 * time.Minute),
		"future boundary": now.Add(15 * time.Minute),
	} {
		request.Header.Set("X-Amz-Date", at.Format("20060102T150405Z"))
		if fault := S3AuthorizationTimeFault(request, now); fault != nil {
			t.Fatalf("%s rejected: %#v", name, fault)
		}
	}
	request.Header.Set("X-Amz-Date", now.Add(-15*time.Minute-time.Second).Format("20060102T150405Z"))
	if fault := S3AuthorizationTimeFault(request, now); fault == nil || fault.Code != "RequestTimeTooSkewed" || fault.HTTPStatus != http.StatusForbidden || fault.Fields["RequestTime"] == nil || fault.Fields["ServerTime"] == nil {
		t.Fatalf("past skew fault = %#v", fault)
	}
	request.Header.Set("X-Amz-Date", now.Add(15*time.Minute+time.Second).Format("20060102T150405Z"))
	if fault := S3AuthorizationTimeFault(request, now); fault == nil || fault.Code != "RequestTimeTooSkewed" {
		t.Fatalf("future skew fault = %#v", fault)
	}
	request.Header.Set("X-Amz-Date", "invalid")
	if fault := S3AuthorizationTimeFault(request, now); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("invalid timestamp fault = %#v", fault)
	}
	request.Header.Set("Authorization", "AWS test:signature")
	request.Header.Set("X-Amz-Date", now.Format(http.TimeFormat))
	if fault := S3AuthorizationTimeFault(request, now); fault != nil {
		t.Fatalf("SigV2 x-amz-date rejected: %#v", fault)
	}
	request.Header.Del("X-Amz-Date")
	request.Header.Set("Date", now.Format(http.TimeFormat))
	if fault := S3AuthorizationTimeFault(request, now); fault != nil {
		t.Fatalf("SigV2 Date rejected: %#v", fault)
	}
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set("X-Amz-Date", "invalid")
	if fault := S3AuthorizationTimeFault(request, now); fault != nil {
		t.Fatalf("unrelated authorization rejected: %#v", fault)
	}
}

func TestVerifyS3StreamingV4AWSExample(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "https://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	request.ContentLength = 66824
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	request.Header.Set("X-Amz-Date", "20130524T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	request.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if fault := VerifyS3AuthorizationV4(request, secret); fault != nil {
		t.Fatalf("official AWS seed signature rejected: %#v", fault)
	}
	chunks := [][]byte{bytes.Repeat([]byte{'a'}, 64*1024), bytes.Repeat([]byte{'a'}, 1024), nil}
	signatures := []string{"ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648", "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497", "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"}
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, nil); fault != nil {
		t.Fatalf("official AWS chunk signatures rejected: %#v", fault)
	}
	if fault := VerifyS3StreamingV4(request, secret, chunks[:2], signatures[:2], nil); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("stream without final chunk accepted: %#v", fault)
	}
	chunks[1][0] = 'b'
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, nil); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed chunk accepted: %#v", fault)
	}
}

func TestVerifyS3StreamingV4A(t *testing.T) {
	const accessKey = "AKISORANDOMAASORANDOM"
	const secret = "q+jcrXGc+0zWN6uzclKVhvMmUsIfRPa4rlRandom"
	const date = "20990101T000000Z"
	credential := []string{accessKey, "20990101", "s3", "aws4_request"}
	key := s3V4AKey(accessKey, secret)
	sign := func(t *testing.T, stringToSign string, padded bool) string {
		t.Helper()
		digest := sha256.Sum256([]byte(stringToSign))
		var hexSignature string
		for entropy := byte(0x42); ; entropy++ {
			encoded, err := ecdsa.SignASN1(bytes.NewReader(bytes.Repeat([]byte{entropy}, 1024)), key, digest[:])
			if err != nil {
				t.Fatal(err)
			}
			hexSignature = hex.EncodeToString(encoded)
			if !padded || len(hexSignature) < 144 || entropy == 0xff {
				break
			}
		}
		if padded {
			hexSignature = strings.Repeat("*", 144-len(hexSignature)) + hexSignature
		}
		return hexSignature
	}
	for _, mode := range []string{"payload", "trailer", "unsigned-trailer"} {
		t.Run(mode, func(t *testing.T) {
			trailerMode := mode != "payload"
			unsignedTrailerMode := mode == "unsigned-trailer"
			payloadHash := "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
			signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set"
			if unsignedTrailerMode {
				payloadHash = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
			} else if trailerMode {
				payloadHash += "-TRAILER"
			}
			if trailerMode {
				signedHeaders += ";x-amz-trailer"
			}
			request := httptest.NewRequest(http.MethodPut, "https://examplebucket.s3.amazonaws.com/object", nil)
			request.Header.Set("Content-Encoding", "aws-chunked")
			request.Header.Set("X-Amz-Content-Sha256", payloadHash)
			request.Header.Set("X-Amz-Date", date)
			request.Header.Set("X-Amz-Decoded-Content-Length", "5")
			request.Header.Set("X-Amz-Region-Set", "us-east-1")
			if trailerMode {
				request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			}
			canonicalHeaders, ok := signedHeaderValues(request, strings.Split(signedHeaders, ";"))
			if !ok {
				t.Fatal("invalid test headers")
			}
			canonical := strings.Join([]string{request.Method, canonicalPath(request.URL), "", canonicalHeaders, signedHeaders, payloadHash}, "\n")
			requestHash := sha256.Sum256([]byte(canonical))
			scope := strings.Join(credential[1:], "/")
			seed := sign(t, strings.Join([]string{s3V4AAlgorithm, date, scope, hex.EncodeToString(requestHash[:])}, "\n"), false)
			request.Header.Set("Authorization", s3V4AAlgorithm+" Credential="+strings.Join(credential, "/")+",SignedHeaders="+signedHeaders+",Signature="+seed)
			if fault := VerifyS3V4A(request, accessKey, secret, "us-east-1"); fault != nil {
				t.Fatalf("stream seed rejected: %#v", fault)
			}
			chunks := [][]byte{[]byte("hello"), nil}
			signatures := make([]string, len(chunks))
			previous := seed
			empty := sha256.Sum256(nil)
			if !unsignedTrailerMode {
				for i, chunk := range chunks {
					chunkHash := sha256.Sum256(chunk)
					payload := strings.Join([]string{strings.TrimLeft(previous, "*"), hex.EncodeToString(empty[:]), hex.EncodeToString(chunkHash[:])}, "\n")
					signatures[i] = sign(t, strings.Join([]string{"AWS4-ECDSA-P256-SHA256-PAYLOAD", date, scope, payload}, "\n"), true)
					previous = signatures[i]
				}
			}
			trailers := http.Header(nil)
			if trailerMode {
				trailers = http.Header{"X-Amz-Checksum-Crc32c": {"mnG7TA=="}}
			}
			if trailerMode && !unsignedTrailerMode {
				trailers.Set("X-Amz-Trailer-Signature", strings.Repeat("0", 144))
				canonicalTrailer, ok := canonicalS3StreamingTrailers(request, trailers, true)
				if !ok {
					t.Fatal("invalid test trailers")
				}
				trailerHash := sha256.Sum256([]byte(canonicalTrailer))
				payload := strings.TrimLeft(previous, "*") + "\n" + hex.EncodeToString(trailerHash[:])
				trailers.Set("X-Amz-Trailer-Signature", sign(t, strings.Join([]string{"AWS4-ECDSA-P256-SHA256-TRAILER", date, scope, payload}, "\n"), true))
			}
			if fault := VerifyS3StreamingSignature(request, accessKey, secret, chunks, signatures, trailers); fault != nil {
				t.Fatalf("valid SigV4A stream rejected: %#v", fault)
			}
			if unsignedTrailerMode {
				authorization := request.Header.Get("Authorization")
				request.Header.Set("Authorization", strings.Replace(authorization, ";x-amz-trailer", "", 1))
				if fault := VerifyS3StreamingSignature(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("unsigned SigV4A trailer declaration accepted: %#v", fault)
				}
				request.Header.Set("Authorization", authorization)
				signatures[0] = "unexpected"
				if fault := VerifyS3StreamingSignature(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("unsigned SigV4A stream with chunk signature accepted: %#v", fault)
				}
				signatures[0] = ""
				trailers.Set("X-Amz-Extra", "value")
				if fault := VerifyS3StreamingSignature(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("undeclared unsigned SigV4A trailer accepted: %#v", fault)
				}
				return
			}
			chunks[0][0] = 'j'
			if fault := VerifyS3StreamingSignature(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("dispatcher accepted tampered SigV4A chunk: %#v", fault)
			}
			if fault := VerifyS3StreamingV4A(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("tampered SigV4A chunk accepted: %#v", fault)
			}
			chunks[0][0] = 'h'
			if trailerMode {
				authorization := request.Header.Get("Authorization")
				request.Header.Set("Authorization", strings.Replace(authorization, ";x-amz-trailer", "", 1))
				if fault := VerifyS3StreamingV4A(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("unsigned trailer declaration accepted: %#v", fault)
				}
				request.Header.Set("Authorization", authorization)
				extra := trailers.Clone()
				extra.Set("X-Amz-Extra", "value")
				if fault := VerifyS3StreamingV4A(request, accessKey, secret, chunks, signatures, extra); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("undeclared trailer accepted: %#v", fault)
				}
				trailers.Set("X-Amz-Checksum-Crc32c", "AAAAAA==")
				if fault := VerifyS3StreamingV4A(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("tampered SigV4A trailer accepted: %#v", fault)
				}
				trailers.Set("X-Amz-Checksum-Crc32c", "mnG7TA==")
				trailerSignature := trailers.Get("X-Amz-Trailer-Signature")
				trailers.Set("X-Amz-Trailer-Signature", trailerSignature[1:])
				if fault := VerifyS3StreamingV4A(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
					t.Fatalf("unpadded SigV4A trailer accepted: %#v", fault)
				}
				trailers.Set("X-Amz-Trailer-Signature", trailerSignature)
			}
			signatures[0] = signatures[0][1:]
			if fault := VerifyS3StreamingV4A(request, accessKey, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("unpadded SigV4A chunk accepted: %#v", fault)
			}
		})
	}
}

func TestVerifyS3StreamingTrailerV4AWSExample(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "https://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	request.ContentLength = 66946
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20130524T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	request.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer,Signature=106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e")
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if fault := VerifyS3AuthorizationV4(request, secret); fault != nil {
		t.Fatalf("official AWS trailer seed signature rejected: %#v", fault)
	}
	chunks := [][]byte{bytes.Repeat([]byte{'a'}, 64*1024), bytes.Repeat([]byte{'a'}, 1024), nil}
	signatures := []string{"b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2", "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7", "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"}
	trailers := http.Header{"X-Amz-Checksum-Crc32c": {"sOO8/Q=="}, "X-Amz-Trailer-Signature": {"d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435"}}
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, trailers); fault != nil {
		t.Fatalf("official AWS trailer signature rejected: %#v", fault)
	}
	authorization := request.Header.Get("Authorization")
	request.Header.Set("Authorization", strings.Replace(authorization, ";x-amz-trailer", "", 1))
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("unsigned trailer declaration accepted: %#v", fault)
	}
	request.Header.Set("Authorization", authorization)
	extra := trailers.Clone()
	extra.Set("X-Amz-Extra", "value")
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, extra); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("undeclared trailer accepted: %#v", fault)
	}
	trailers.Set("X-Amz-Checksum-Crc32c", "AAAAAA==")
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed trailer accepted: %#v", fault)
	}
}

func TestVerifyS3StreamingUnsignedTrailerV4(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "https://s3.localhost.localstack.cloud:4566/unsigned/object", nil)
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=fcefc9ae2b8230495738dd184bf82843d23e54dc536efdf1dcdd0acb7fe9277a")
	if fault := VerifyS3AuthorizationV4(request, "test"); fault != nil {
		t.Fatalf("unsigned trailer seed signature rejected: %#v", fault)
	}
	chunks := [][]byte{[]byte("hello"), nil}
	signatures := []string{"", ""}
	trailers := http.Header{"X-Amz-Checksum-Crc32c": {"mnG7TA=="}}
	if fault := VerifyS3StreamingV4(request, "test", chunks, signatures, trailers); fault != nil {
		t.Fatalf("unsigned trailer rejected: %#v", fault)
	}
	signatures[0] = "unexpected"
	if fault := VerifyS3StreamingV4(request, "test", chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("unsigned stream with chunk signature accepted: %#v", fault)
	}
	signatures[0] = ""
	trailers.Set("X-Amz-Extra", "value")
	if fault := VerifyS3StreamingV4(request, "test", chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("undeclared unsigned trailer accepted: %#v", fault)
	}
}

func awsAuthorizationExample() *http.Request {
	request := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	request.Header.Set("Range", "bytes=0-9")
	request.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	request.Header.Set("X-Amz-Date", "20130524T000000Z")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	return request
}

func TestVerifyS3PresignedV2AWSGrammar(t *testing.T) {
	// AWS documents this exact query-auth string-to-sign grammar. The
	// published signature uses unrelated legacy credentials, so this fixture's
	// signature was independently reproduced with Python's stdlib HMAC-SHA1.
	const rawURL = "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Expires=1175139620&Signature=1No4mq5ETf02z8aet9voy6gui6E%3D"
	request := httptest.NewRequest(http.MethodGet, rawURL, nil)
	if fault := VerifyS3PresignedV2(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault != nil {
		t.Fatalf("AWS grammar fixture rejected: %#v", fault)
	}
	query := request.URL.Query()
	query.Set("Signature", "AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	request.URL.RawQuery = query.Encode()
	if fault := VerifyS3PresignedV2(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signature accepted: %#v", fault)
	}
	pathStyle := httptest.NewRequest(http.MethodGet, "https://localhost/bucket/key?acl&AWSAccessKeyId=test&Expires=4070908800&Signature=wUhjtATf5qx6xYeZ1XjoZQHJKjg%3D", nil)
	if fault := VerifyS3PresignedV2(pathStyle, "test"); fault != nil {
		t.Fatalf("path-style subresource rejected: %#v", fault)
	}
}

func TestVerifyS3SessionToken(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://localhost/key?X-Amz-Security-Token=session", nil)
	if fault := VerifyS3SessionToken(request, "session"); fault != nil {
		t.Fatalf("valid query token rejected: %#v", fault)
	}
	request.URL.RawQuery = ""
	request.Header.Set("X-Amz-Security-Token", "session")
	if fault := VerifyS3SessionToken(request, "session"); fault != nil {
		t.Fatalf("valid header token rejected: %#v", fault)
	}
	request.Header.Del("X-Amz-Security-Token")
	if fault := VerifyS3SessionToken(request, "session"); fault == nil || fault.Code != "InvalidToken" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing token accepted: %#v", fault)
	}
}
