package cognitoidp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const jwtIAT int64 = 1577836800 // ponytail: seed-stable iat (controllable-clock epoch); wall iat is Clock.Now().Unix().

func (p *Pack) jwtKey() []byte {
	return p.deps.Rand.Derive("cognito-hs256").Bytes(32)
}

func (p *Pack) signJWT(claims map[string]any) string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","kid":"mirror","typ":"JWT"}`))
	raw, _ := json.Marshal(claims)
	pay := base64.RawURLEncoding.EncodeToString(raw)
	msg := hdr + "." + pay
	mac := hmac.New(sha256.New, p.jwtKey())
	mac.Write([]byte(msg))
	return msg + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (p *Pack) accessJWT(req *spi.Request, pool, cid, name string) string {
	iss := "https://cognito-idp." + req.Identity.Region + ".amazonaws.com/" + req.Identity.Region + "_" + pool
	return p.signJWT(map[string]any{
		"auth_time": jwtIAT, "client_id": cid, "exp": jwtIAT + 3600, "iat": jwtIAT,
		"iss": iss, "jti": p.deps.Rand.Derive("jti-access/" + pool + "/" + name).Hex(16),
		"scope": "aws.cognito.signin.user.admin", "sub": name, "token_use": "access", "username": name,
	})
}

func (p *Pack) idJWT(req *spi.Request, pool, cid, name string) string {
	iss := "https://cognito-idp." + req.Identity.Region + ".amazonaws.com/" + req.Identity.Region + "_" + pool
	return p.signJWT(map[string]any{
		"aud": cid, "auth_time": jwtIAT, "cognito:username": name, "exp": jwtIAT + 3600, "iat": jwtIAT,
		"iss": iss, "jti": p.deps.Rand.Derive("jti-id/" + pool + "/" + name).Hex(16),
		"sub": name, "token_use": "id",
	})
}
