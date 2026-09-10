package cert

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"strings"
	"sync"
	"time"
)

var signing struct {
	once sync.Once
	key  *rsa.PrivateKey
	cert []byte
}

type deterministicReader struct {
	seed []byte
	n    uint64
	buf  []byte
}

func (r *deterministicReader) Read(p []byte) (int, error) {
	for len(r.buf) < len(p) {
		var counter [8]byte
		binary.BigEndian.PutUint64(counter[:], r.n)
		sum := sha256.Sum256(append(append([]byte{}, r.seed...), counter[:]...))
		r.buf = append(r.buf, sum[:]...)
		r.n++
	}
	copy(p, r.buf[:len(p)])
	r.buf = r.buf[len(p):]
	return len(p), nil
}

func initSigning() {
	// ponytail: deterministic local key keeps characterization snapshots stable; rotate only for external trust.
	reader := &deterministicReader{seed: []byte("mirror.cloud SNS signing key")}
	signing.key, _ = rsa.GenerateKey(reader, 2048)
	notBefore := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Mirror SNS"}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, IsCA: true}
	der, _ := x509.CreateCertificate(reader, template, template, &signing.key.PublicKey, signing.key)
	signing.cert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func Certificate() []byte {
	signing.once.Do(initSigning)
	return signing.cert
}

func Sign(values map[string]any, version string, stringify func(any) string) string {
	signing.once.Do(initSigning)
	fields := []string{"Message", "MessageId"}
	if typ := stringify(values["Type"]); typ == "SubscriptionConfirmation" || typ == "UnsubscribeConfirmation" {
		fields = append(fields, "SubscribeURL", "Subject", "Timestamp", "Token", "TopicArn", "Type")
	} else {
		fields = append(fields, "Subject", "Timestamp", "TopicArn", "Type")
	}
	var b strings.Builder
	for _, field := range fields {
		if value, ok := values[field]; ok {
			b.WriteString(field)
			b.WriteByte('\n')
			b.WriteString(stringify(value))
			b.WriteByte('\n')
		}
	}
	var hash crypto.Hash
	var digest []byte
	if version == "2" {
		hash = crypto.SHA256
		sum := sha256.Sum256([]byte(b.String()))
		digest = sum[:]
	} else {
		hash = crypto.SHA1
		sum := sha1.Sum([]byte(b.String()))
		digest = sum[:]
	}
	signature, _ := rsa.SignPKCS1v15(rand.Reader, signing.key, hash, digest)
	return base64.StdEncoding.EncodeToString(signature)
}
