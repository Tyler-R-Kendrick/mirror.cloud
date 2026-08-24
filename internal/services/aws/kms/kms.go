// Package kms is a local AES-GCM emulate of AWS KMS (not HSM, not AWS-compatible ciphertext).
package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.kms", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements KMS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.kms" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return append([]string{"CreateKey", "DescribeKey", "ListKeys", "Encrypt", "Decrypt", "GenerateDataKey"}, extraOps()...)
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("kms")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateKey":
		id := p.deps.Rand.Hex(8)
		mat := p.deps.Rand.Bytes(32)
		arn := "arn:aws:kms:" + req.Identity.Region + ":" + req.Identity.Account + ":key/" + id
		rec := map[string]any{"KeyId": id, "Arn": arn, "KeyMaterial": base64.StdEncoding.EncodeToString(mat), "KeyState": "Enabled"}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"KeyMetadata": map[string]any{"KeyId": id, "Arn": arn, "KeyState": "Enabled"}}}, nil
	case "DescribeKey":
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		b, ok, _ := p.col(req).Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		delete(rec, "KeyMaterial")
		return &spi.Response{Output: map[string]any{"KeyMetadata": rec}}, nil
	case "ListKeys":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var keys []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			keys = append(keys, map[string]any{"KeyId": rec["KeyId"], "KeyArn": rec["Arn"]})
		}
		return &spi.Response{Output: map[string]any{"Keys": keys}}, nil
	case "Encrypt":
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		mat, err := p.material(ctx, req, id)
		if err != nil {
			return nil, err
		}
		pt := blob(req.Input["Plaintext"])
		ct, err := seal(mat, pt)
		if err != nil {
			return nil, err
		}
		wrapped := append([]byte(id+"|"), ct...)
		return &spi.Response{Output: map[string]any{"CiphertextBlob": wrapped, "KeyId": id}}, nil
	case "Decrypt":
		raw := blob(req.Input["CiphertextBlob"])
		id, ct, ok := splitID(raw)
		if !ok {
			id = p.resolve(ctx, req, str(req.Input["KeyId"]))
			ct = raw
		} else {
			id = p.resolve(ctx, req, id)
		}
		mat, err := p.material(ctx, req, id)
		if err != nil {
			return nil, err
		}
		pt, err := open(mat, ct)
		if err != nil {
			return nil, &spi.Fault{Code: "InvalidCiphertextException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"Plaintext": pt, "KeyId": id}}, nil
	case "GenerateDataKey":
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		mat, err := p.material(ctx, req, id)
		if err != nil {
			return nil, err
		}
		pt := p.deps.Rand.Bytes(32)
		ct, err := seal(mat, pt)
		if err != nil {
			return nil, err
		}
		wrapped := append([]byte(id+"|"), ct...)
		return &spi.Response{Output: map[string]any{"KeyId": id, "Plaintext": pt, "CiphertextBlob": wrapped}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) material(ctx context.Context, req *spi.Request, id string) ([]byte, error) {
	b, ok, _ := p.col(req).Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	if st := str(rec["KeyState"]); st == "Disabled" || st == "PendingDeletion" {
		return nil, &spi.Fault{Code: "DisabledException", HTTPStatus: 400, Fault: "client"}
	}
	s, _ := rec["KeyMaterial"].(string)
	mat, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(mat) != 32 {
		return nil, &spi.Fault{Code: "InvalidKeyUsageException", HTTPStatus: 400, Fault: "client"}
	}
	return mat, nil
}

func keyID(req *spi.Request) string {
	s, _ := req.Input["KeyId"].(string)
	if i := lastSlash(s); i >= 0 {
		return s[i+1:]
	}
	return s
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func blob(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		if b, err := base64.StdEncoding.DecodeString(t); err == nil {
			return b
		}
		return []byte(t)
	}
	return nil
}

func splitID(raw []byte) (id string, ct []byte, ok bool) {
	for i := 0; i < len(raw) && i < 16; i++ {
		if raw[i] == '|' {
			return string(raw[:i]), raw[i+1:], true
		}
	}
	return "", raw, false
}

func seal(key, pt []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := gcm.Seal(nonce, nonce, pt, nil)
	return out, nil
}

func open(key, ct []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ns := gcm.NonceSize()
	if len(ct) < ns {
		return nil, &spi.Fault{Code: "InvalidCiphertextException", HTTPStatus: 400, Fault: "client"}
	}
	return gcm.Open(nil, ct[:ns], ct[ns:], nil)
}
