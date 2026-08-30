package identity

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
)

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
