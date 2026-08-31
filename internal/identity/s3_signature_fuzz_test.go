package identity

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func FuzzS3AuthorizationTimeFault(f *testing.F) {
	f.Add(uint8(0), "20990101T000000Z", int64(4070908800))
	f.Add(uint8(1), "Thu, 01 Jan 2099 00:00:00 GMT", int64(4070908800))
	f.Fuzz(func(t *testing.T, scheme uint8, date string, now int64) {
		request, err := http.NewRequest(http.MethodGet, "https://example.test", nil)
		if err != nil {
			t.Skip()
		}
		if scheme%2 == 0 {
			request.Header.Set("Authorization", "AWS4-HMAC-SHA256 signed")
		} else {
			request.Header.Set("Authorization", "AWS test:signature")
		}
		request.Header.Set("X-Amz-Date", date)
		_ = S3AuthorizationTimeFault(request, time.Unix(now, 0))
	})
}

func FuzzVerifyS3PresignedV4(f *testing.F) {
	f.Add("GET", "/test.txt", "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404", "examplebucket.s3.amazonaws.com", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	f.Fuzz(func(t *testing.T, method, path, signature, host, secret string) {
		if method == "" || path == "" || path[0] != '/' {
			t.Skip()
		}
		target := "https://example.test" + path + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=" + signature
		request, err := http.NewRequest(method, target, nil)
		if err != nil {
			t.Skip()
		}
		request.Host = host
		_ = VerifyS3PresignedV4(request, secret)
	})
}

func FuzzVerifyS3AuthorizationV4(f *testing.F) {
	f.Add("GET", "/test.txt", "host;x-amz-content-sha256;x-amz-date", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", "examplebucket.s3.amazonaws.com", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	f.Fuzz(func(t *testing.T, method, path, signedHeaders, signature, host, secret string) {
		if method == "" || path == "" || path[0] != '/' {
			t.Skip()
		}
		request, err := http.NewRequest(method, "https://example.test"+path, nil)
		if err != nil {
			t.Skip()
		}
		request.Host = host
		request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		request.Header.Set("X-Amz-Date", "20130524T000000Z")
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20130524/us-east-1/s3/aws4_request,SignedHeaders="+signedHeaders+",Signature="+signature)
		_ = VerifyS3AuthorizationV4(request, secret)
	})
}

func FuzzVerifyS3V4A(f *testing.F) {
	f.Add(false, "GET", "/test.txt", "host;x-amz-content-sha256;x-amz-date;x-amz-region-set", "00", "us-east-1", "examplebucket.s3.amazonaws.com", "test")
	f.Add(true, "GET", "/test.txt", "host", "00", "us-east-1,*", "examplebucket.s3.amazonaws.com", "test")
	f.Fuzz(func(t *testing.T, query bool, method, path, signedHeaders, signature, regionSet, host, secret string) {
		if method == "" || path == "" || path[0] != '/' {
			t.Skip()
		}
		request, err := http.NewRequest(method, "https://example.test"+path, nil)
		if err != nil {
			t.Skip()
		}
		request.Host = host
		if query {
			values := request.URL.Query()
			values.Set("X-Amz-Algorithm", s3V4AAlgorithm)
			values.Set("X-Amz-Credential", "test/20130524/s3/aws4_request")
			values.Set("X-Amz-Date", "20130524T000000Z")
			values.Set("X-Amz-Expires", "60")
			values.Set("X-Amz-Region-Set", regionSet)
			values.Set("X-Amz-SignedHeaders", signedHeaders)
			values.Set("X-Amz-Signature", signature)
			request.URL.RawQuery = values.Encode()
		} else {
			request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			request.Header.Set("X-Amz-Date", "20130524T000000Z")
			request.Header.Set("X-Amz-Region-Set", regionSet)
			request.Header.Set("Authorization", s3V4AAlgorithm+" Credential=test/20130524/s3/aws4_request,SignedHeaders="+signedHeaders+",Signature="+signature)
		}
		_ = VerifyS3V4A(request, "test", secret, "us-east-1")
	})
}

func FuzzVerifyS3AuthorizationV2(f *testing.F) {
	f.Add("GET", "/photos/puppy.jpg", "AKIAIOSFODNN7EXAMPLE", "qgk2+6Sv9/oM7G3qLEjTH1a1l1g=", "awsexamplebucket1.s3.us-west-1.amazonaws.com", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	f.Fuzz(func(t *testing.T, method, path, accessKey, signature, host, secret string) {
		if method == "" || path == "" || path[0] != '/' {
			t.Skip()
		}
		request, err := http.NewRequest(method, "https://example.test"+path, nil)
		if err != nil {
			t.Skip()
		}
		request.Host = host
		request.Header.Set("Date", "Tue, 27 Mar 2007 19:36:42 +0000")
		request.Header.Set("Authorization", "AWS "+accessKey+":"+signature)
		_ = VerifyS3AuthorizationV2(request, secret)
	})
}

func FuzzVerifyS3PostPolicy(f *testing.F) {
	f.Add(true, "cG9saWN5", "test/20990101/us-east-1/s3/aws4_request", "20990101T000000Z", "signature", "test")
	f.Add(false, "cG9saWN5", "test", "", "signature", "test")
	f.Fuzz(func(t *testing.T, v4 bool, policy, credential, date, signature, secret string) {
		fields := map[string]string{"policy": policy}
		if v4 {
			fields["x-amz-algorithm"] = "AWS4-HMAC-SHA256"
			fields["x-amz-credential"] = credential
			fields["x-amz-date"] = date
			fields["x-amz-signature"] = signature
		} else {
			fields["AWSAccessKeyId"] = credential
			fields["signature"] = signature
		}
		_ = VerifyS3PostPolicy(fields, secret)
	})
}

func FuzzVerifyS3StreamingV4(f *testing.F) {
	f.Add([]byte("hello"), "87081aa8d08ebfccd3aa73e18ac88541cf2050c23a5a49a9e46d94a70d84f2a4", "eaf2700e23d624c531f0f9a0c7312b66470ab3aee81742bfa00dfc9cf6ca0f4e", "test")
	f.Fuzz(func(t *testing.T, data []byte, signature, finalSignature, secret string) {
		request, err := http.NewRequest(http.MethodPut, "https://s3.localhost.localstack.cloud:4566/streaming/object", nil)
		if err != nil {
			t.Skip()
		}
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Date", "20990101T000000Z")
		request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(data)))
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=d32bab45d70b05d89ada2e57acc27c4117cf31f7ce3de470cf916b8f89558054")
		_ = VerifyS3StreamingV4(request, secret, [][]byte{data, nil}, []string{signature, finalSignature}, nil)
	})
}

func FuzzVerifyS3StreamingTrailerV4(f *testing.F) {
	f.Add("x-amz-checksum-crc32c", "mnG7TA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa", "test")
	f.Fuzz(func(t *testing.T, name, value, trailerSignature, secret string) {
		request, err := http.NewRequest(http.MethodPut, "https://s3.localhost.localstack.cloud:4566/trailers/object", nil)
		if err != nil {
			t.Skip()
		}
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
		request.Header.Set("X-Amz-Date", "20990101T000000Z")
		request.Header.Set("X-Amz-Decoded-Content-Length", "5")
		request.Header.Set("X-Amz-Trailer", name)
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=378380e9501dea596cd83a9661c42fc2603dbd37872ab598316173a4d9244821")
		trailers := http.Header{name: {value}, "X-Amz-Trailer-Signature": {trailerSignature}}
		_ = VerifyS3StreamingV4(request, secret, [][]byte{[]byte("hello"), nil}, []string{"c83b0404927860c2dfacb114cd53dfe5505c5b4ad4dc605cc4e53806d4bb0d74", "ffc89ae66d2e00900ad958aa09d8ea91ab7e1cb1938d6f4a5a30821f8fbe297f"}, trailers)
	})
}

func FuzzVerifyS3StreamingUnsignedTrailerV4(f *testing.F) {
	f.Add([]byte("hello"), "x-amz-checksum-crc32c", "mnG7TA==", false, "test")
	f.Fuzz(func(t *testing.T, data []byte, name, value string, signedChunk bool, secret string) {
		request, err := http.NewRequest(http.MethodPut, "https://s3.localhost.localstack.cloud:4566/unsigned/object", nil)
		if err != nil {
			t.Skip()
		}
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		request.Header.Set("X-Amz-Date", "20990101T000000Z")
		request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(data)))
		request.Header.Set("X-Amz-Trailer", name)
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=fcefc9ae2b8230495738dd184bf82843d23e54dc536efdf1dcdd0acb7fe9277a")
		signature := ""
		if signedChunk {
			signature = "unexpected"
		}
		_ = VerifyS3StreamingV4(request, secret, [][]byte{data, nil}, []string{signature, ""}, http.Header{name: {value}})
	})
}

func FuzzVerifyS3StreamingV4A(f *testing.F) {
	f.Add(false, []byte("hello"), "**304502201ba0be85f07d901a715f28fbcd6d4ee4d14ab70abe11f5cfaff93a3c1961e4ae022100f5693b9c34d100107df15bd06cbc5c1a608d467761f97f26e048c240b21cc256", "**304502202bed57aec7b9b53cfebdf5163fbc5c61009c0f0b1e1b50848ac50641c6d0d14a022100806a00edfb80226cf9f2761851cd38cb9f33ee3fdafb597c723086655aad5cb9", "", "", "", "test")
	f.Add(true, []byte("hello"), "**3045022014ec32c1ce4d72ad9504db7c3584cdf88ef5408590472dfa1333f3696d030a76022100e15554ef66351e5f90b6b9a62e67b0fdf0b2e678ce3c5394252f3e57d93275a6", "**304502210090e80732fa8c16e01818cafdbff64c37e56feced7c512cd43c48481df98377970220145d5e04288392f3bad2740bc847b217751f666baad7ee1a5358c68161b9297d", "x-amz-checksum-crc32c", "mnG7TA==", "****30440220053b683045656f9eba0a1a2785bea923cddca5c5cc83b0d1fba03e1aab23fd5502200c01dde330a75c75412925fe9dd44324a60aee6a7491714e1c1ed6944e0a05aa", "test")
	f.Fuzz(func(t *testing.T, trailer bool, data []byte, signature, finalSignature, trailerName, trailerValue, trailerSignature, secret string) {
		path := "/v4a/object"
		payloadHash := "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
		signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set"
		seed := "30450220292f2afead2f51323260a06fdfed3d88e0998b54f024a175f65e19bdbf970425022100e28adec0e230329184badd9bf335b18c8ad5373000bad0c47223b173ecd16d11"
		trailers := http.Header(nil)
		if trailer {
			path = "/v4a-trailers/object"
			payloadHash += "-TRAILER"
			signedHeaders += ";x-amz-trailer"
			seed = "3046022100dcdd29ee9c78fdb87571b7ee2f202417795100fc3782a87296d8dbcdfd05ee91022100e72c624e7c065de7d9d6bc9f44b805390367f72d041219ea147ec45c4d47d180"
			trailers = http.Header{trailerName: {trailerValue}, "X-Amz-Trailer-Signature": {trailerSignature}}
		}
		request, err := http.NewRequest(http.MethodPut, "https://s3.localhost.localstack.cloud:4566"+path, nil)
		if err != nil {
			t.Skip()
		}
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", payloadHash)
		request.Header.Set("X-Amz-Date", "20990101T000000Z")
		request.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(data)))
		request.Header.Set("X-Amz-Region-Set", "us-east-1")
		if trailer {
			request.Header.Set("X-Amz-Trailer", trailerName)
		}
		request.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders="+signedHeaders+",Signature="+seed)
		_ = VerifyS3StreamingSignature(request, "test", secret, [][]byte{data, nil}, []string{signature, finalSignature}, trailers)
	})
}

func FuzzVerifyS3PresignedV2(f *testing.F) {
	f.Add("GET", "/bucket/key", "signature", "localhost", "test")
	f.Fuzz(func(t *testing.T, method, path, signature, host, secret string) {
		if method == "" || path == "" || path[0] != '/' {
			t.Skip()
		}
		request, err := http.NewRequest(method, "https://example.test"+path+"?AWSAccessKeyId=test&Expires=4070908800&Signature="+url.QueryEscape(signature), nil)
		if err != nil {
			t.Skip()
		}
		request.Host = host
		_ = VerifyS3PresignedV2(request, secret)
	})
}

func FuzzVerifyS3SessionToken(f *testing.F) {
	f.Add("session", "session")
	f.Fuzz(func(t *testing.T, token, expected string) {
		request, err := http.NewRequest(http.MethodGet, "https://example.test/key?X-Amz-Security-Token="+url.QueryEscape(token), nil)
		if err != nil {
			t.Skip()
		}
		_ = VerifyS3SessionToken(request, expected)
	})
}
