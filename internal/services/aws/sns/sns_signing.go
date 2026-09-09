package sns

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

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

var snsSigning struct {
	once sync.Once
	key  *rsa.PrivateKey
	cert []byte
}

func initSNSSigning() {
	snsSigning.key, _ = rsa.GenerateKey(rand.Reader, 2048)
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Mirror SNS"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, template, template, &snsSigning.key.PublicKey, snsSigning.key)
	snsSigning.cert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func signingCertificate() []byte {
	snsSigning.once.Do(initSNSSigning)
	return snsSigning.cert
}

// SigningCertificate returns the certificate used by SNS notification signatures.
func SigningCertificate() []byte { return signingCertificate() }

func signNotification(values map[string]any, version string) string {
	snsSigning.once.Do(initSNSSigning)
	fields := []string{"Message", "MessageId", "Subject", "Timestamp", "TopicArn", "Type"}
	var b strings.Builder
	for _, field := range fields {
		if value, ok := values[field]; ok {
			b.WriteString(field)
			b.WriteByte('\n')
			b.WriteString(str(value))
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
	signature, _ := rsa.SignPKCS1v15(rand.Reader, snsSigning.key, hash, digest)
	return base64.StdEncoding.EncodeToString(signature)
}

func snsCertificateURL(req *spi.Request) string {
	base := strings.TrimRight(req.AdvertiseURL, "/")
	if base == "" {
		base = "http://127.0.0.1:4566"
	}
	return base + "/_aws/sns/SimpleNotificationService.pem"
}
