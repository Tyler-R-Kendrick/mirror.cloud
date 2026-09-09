package sns

import (
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns/cert"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func signingCertificate() []byte { return cert.Certificate() }

// SigningCertificate returns the certificate used by SNS notification signatures.
func SigningCertificate() []byte { return signingCertificate() }

func signNotification(values map[string]any, version string) string {
	return cert.Sign(values, version, str)
}

func snsCertificateURL(req *spi.Request) string {
	base := strings.TrimRight(req.AdvertiseURL, "/")
	if base == "" {
		base = "http://127.0.0.1:4566"
	}
	return base + "/_aws/sns/SimpleNotificationService.pem"
}
