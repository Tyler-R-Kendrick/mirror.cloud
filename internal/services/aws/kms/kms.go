// Package kms is KMS's cryptography: the operations behavior/aws/kms lists as
// native. It is a local AES-GCM emulate (not HSM, not AWS-compatible
// ciphertext) under the key material the bundle stores on each key record;
// keys, aliases, grants and policies are the bundle's.
package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, op := range []string{"Encrypt", "Decrypt", "GenerateDataKey", "GenerateDataKeyWithoutPlaintext",
		"GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext", "ReEncrypt", "Sign", "Verify",
		"GenerateMac", "VerifyMac", "DeriveSharedSecret", "GenerateRandom", "GetParametersForImport", "ImportKeyMaterial"} {
		bundled.RegisterNative("aws.kms", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return (&crypto{deps}).Invoke(ctx, req)
		})
	}
}

// crypto serves the native operations.
type crypto struct{ deps spi.Deps }

func (p *crypto) col(req *spi.Request) spi.Collection { return p.extraCol(req, "kms") }

func (p *crypto) extraCol(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *crypto) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch op {
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
	case "GenerateRandom":
		n := 32
		switch t := req.Input["NumberOfBytes"].(type) {
		case float64:
			n = int(t)
		case int:
			n = t
		case json.Number:
			n, _ = strconv.Atoi(string(t))
		case string:
			n, _ = strconv.Atoi(t)
		}
		if n <= 0 {
			n = 32
		}
		return &spi.Response{Output: map[string]any{"Plaintext": p.deps.Rand.Bytes(n)}}, nil
	case "GenerateDataKeyWithoutPlaintext":
		resp, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "GenerateDataKey", Input: req.Input})
		if err != nil {
			return nil, err
		}
		delete(resp.Output, "Plaintext")
		return resp, nil
	case "GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext":
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		mat, err := p.material(ctx, req, id)
		if err != nil {
			return nil, err
		}
		priv := p.deps.Rand.Bytes(32)
		pub := p.deps.Rand.Bytes(32)
		ct, err := seal(mat, priv)
		if err != nil {
			return nil, err
		}
		wrapped := append([]byte(id+"|"), ct...)
		out := map[string]any{"KeyId": id, "PrivateKeyCiphertextBlob": wrapped, "PublicKey": pub}
		if op == "GenerateDataKeyPair" {
			out["PrivateKeyPlaintext"] = priv
		}
		return &spi.Response{Output: out}, nil
	case "GenerateMac":
		mac, err := p.hmacMsg(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Mac": mac, "KeyId": p.resolve(ctx, req, str(req.Input["KeyId"]))}}, nil
	case "VerifyMac":
		mac, err := p.hmacMsg(ctx, req)
		if err != nil {
			return nil, err
		}
		want := blob(req.Input["Mac"])
		return &spi.Response{Output: map[string]any{"MacValid": hmac.Equal(mac, want), "KeyId": p.resolve(ctx, req, str(req.Input["KeyId"]))}}, nil
	case "Sign":
		// ponytail: HMAC-SHA256 over Message, not RSA/ECDSA. Upgrade if SigningAlgorithm is RSASSA_*.
		mac, err := p.hmacMsg(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Signature": mac, "KeyId": p.resolve(ctx, req, str(req.Input["KeyId"])), "SigningAlgorithm": first(req.Input, "SigningAlgorithm")}}, nil
	case "Verify":
		mac, err := p.hmacMsg(ctx, req)
		if err != nil {
			return nil, err
		}
		sig := blob(req.Input["Signature"])
		return &spi.Response{Output: map[string]any{"SignatureValid": hmac.Equal(mac, sig), "KeyId": p.resolve(ctx, req, str(req.Input["KeyId"]))}}, nil
	case "GetParametersForImport":
		return &spi.Response{Output: map[string]any{
			"KeyId":             p.resolve(ctx, req, str(req.Input["KeyId"])),
			"ImportToken":       p.deps.Rand.Bytes(16),
			"PublicKey":         p.deps.Rand.Bytes(32),
			"ParametersValidTo": "2020-01-08T00:00:00Z",
		}}, nil
	case "DeriveSharedSecret":
		// ponytail: XOR of key material with PublicKey bytes, not ECDH.
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		mat, err := p.material(ctx, req, id)
		if err != nil {
			return nil, err
		}
		pub := blob(req.Input["PublicKey"])
		out := make([]byte, len(mat))
		copy(out, mat)
		for i := 0; i < len(out) && i < len(pub); i++ {
			out[i] ^= pub[i]
		}
		return &spi.Response{Output: map[string]any{"SharedSecret": out, "KeyId": id}}, nil
	case "ReEncrypt":
		dec, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "Decrypt", Input: map[string]any{"CiphertextBlob": req.Input["CiphertextBlob"]}})
		if err != nil {
			return nil, err
		}
		dest := first(req.Input, "DestinationKeyId")
		enc, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Operation: "Encrypt", Input: map[string]any{"KeyId": dest, "Plaintext": dec.Output["Plaintext"]}})
		if err != nil {
			return nil, err
		}
		enc.Output["SourceKeyId"] = dec.Output["KeyId"]
		enc.Output["KeyId"] = dest
		return enc, nil
	case "ImportKeyMaterial":
		rec, err := p.loadKey(ctx, req)
		if err != nil {
			return nil, err
		}
		if mat := blob(req.Input["EncryptedKeyMaterial"]); len(mat) == 32 {
			rec["KeyMaterial"] = hex.EncodeToString(mat)
		}
		rec["Imported"] = true
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, str(rec["KeyId"]), b)
		return &spi.Response{Output: map[string]any{"KeyId": rec["KeyId"]}}, nil
	default:
		return nil, spi.NotImplemented("aws.kms", op, "emulate")
	}
}

func (p *crypto) material(ctx context.Context, req *spi.Request, id string) ([]byte, error) {
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
	mat, err := hex.DecodeString(s)
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

func (p *crypto) resolve(ctx context.Context, req *spi.Request, id string) string {
	if strings.HasPrefix(id, "alias/") || strings.Contains(id, ":alias/") {
		name := id
		if i := strings.Index(id, "alias/"); i >= 0 {
			name = id[i:]
		}
		if rec, ok := p.loadExtra(ctx, req, "kmsalias", name); ok {
			if t := str(rec["TargetKeyId"]); t != "" {
				return t
			}
		}
		return id
	}
	return keyID(&spi.Request{Input: map[string]any{"KeyId": id}})
}

func (p *crypto) loadKey(ctx context.Context, req *spi.Request) (map[string]any, error) {
	id := p.resolve(ctx, req, str(req.Input["KeyId"]))
	b, ok, _ := p.col(req).Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, nil
}

func (p *crypto) hmacMsg(ctx context.Context, req *spi.Request) ([]byte, error) {
	id := p.resolve(ctx, req, str(req.Input["KeyId"]))
	mat, err := p.material(ctx, req, id)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, mat)
	mac.Write(blob(req.Input["Message"]))
	return mac.Sum(nil), nil
}

func (p *crypto) loadExtra(ctx context.Context, req *spi.Request, col, id string) (map[string]any, bool) {
	b, ok, _ := p.extraCol(req, col).Get(ctx, id)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}
