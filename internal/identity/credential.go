package identity

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// S3Credential answers the secret an access key signs with and, for a
// temporary credential, the session token it must present. STS and S3's
// CreateSession store each temporary credential they issue in the global
// "stsk" collection, keyed by access key; every other key's secret is derived
// from the key, which is what IAM's CreateAccessKey hands out.
func S3Credential(ctx context.Context, store spi.Store, rnd spi.Rand, accessKey string) (secret, token string, temporary bool) {
	if accessKey == "test" {
		return "test", "", false
	}
	if b, ok, _ := store.Scope("_mirror", "global").Collection("stsk").Get(ctx, accessKey); ok {
		var rec struct{ SecretAccessKey, SessionToken string }
		_ = json.Unmarshal(b, &rec)
		return rec.SecretAccessKey, rec.SessionToken, true
	}
	return rnd.Derive(accessKey).Hex(40), "", false
}
