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

func initSigning() {
	signing.key, _ = rsa.GenerateKey(rand.Reader, 2048)
	notBefore := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Mirror SNS"}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, IsCA: true}
	der, _ := x509.CreateCertificate(rand.Reader, template, template, &signing.key.PublicKey, signing.key)
	signing.cert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func Certificate() []byte {
	signing.once.Do(initSigning)
	return signing.cert
}

func Sign(values map[string]any, version string, stringify func(any) string) string {
	signing.once.Do(initSigning)
	fields := []string{"Message", "MessageId", "Subject", "Timestamp", "TopicArn", "Type"}
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
