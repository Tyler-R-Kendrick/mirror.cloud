package kms

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"CancelKeyDeletion", "ConnectCustomKeyStore", "CreateAlias", "CreateCustomKeyStore", "CreateGrant",
		"DeleteAlias", "DeleteCustomKeyStore", "DeleteImportedKeyMaterial", "DeriveSharedSecret",
		"DescribeCustomKeyStores", "DisableKey", "DisableKeyRotation", "DisconnectCustomKeyStore",
		"EnableKey", "EnableKeyRotation", "GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext",
		"GenerateDataKeyWithoutPlaintext", "GenerateMac", "GenerateRandom", "GetKeyLastUsage", "GetKeyPolicy",
		"GetKeyRotationStatus", "GetParametersForImport", "GetPublicKey", "ImportKeyMaterial", "ListAliases",
		"ListGrants", "ListKeyPolicies", "ListKeyRotations", "ListResourceTags", "ListRetirableGrants",
		"PutKeyPolicy", "ReEncrypt", "ReplicateKey", "RetireGrant", "RevokeGrant", "RotateKeyOnDemand",
		"ScheduleKeyDeletion", "Sign", "TagResource", "UntagResource", "UpdateAlias", "UpdateCustomKeyStore",
		"UpdateKeyDescription", "UpdatePrimaryRegion", "Verify", "VerifyMac",
	}
}

func (p *Pack) extraCol(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch op {
	case "CreateAlias":
		name := str(req.Input["AliasName"])
		rec := map[string]any{"AliasName": name, "TargetKeyId": p.resolve(ctx, req, str(req.Input["TargetKeyId"]))}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "kmsalias").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UpdateAlias":
		name := str(req.Input["AliasName"])
		rec, _ := p.loadExtra(ctx, req, "kmsalias", name)
		if rec == nil {
			rec = map[string]any{"AliasName": name}
		}
		rec["TargetKeyId"] = p.resolve(ctx, req, str(req.Input["TargetKeyId"]))
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "kmsalias").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteAlias":
		_ = p.extraCol(req, "kmsalias").Delete(ctx, str(req.Input["AliasName"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAliases":
		kvs, _, _ := p.extraCol(req, "kmsalias").List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"Aliases": out}}, nil
	case "CreateCustomKeyStore", "UpdateCustomKeyStore":
		id := str(req.Input["CustomKeyStoreId"])
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["CustomKeyStoreId"] = id
		rec["ConnectionState"] = "CONNECTED"
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "kmscks").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"CustomKeyStoreId": id}}, nil
	case "DescribeCustomKeyStores":
		kvs, _, _ := p.extraCol(req, "kmscks").List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"CustomKeyStores": out}}, nil
	case "DeleteCustomKeyStore":
		_ = p.extraCol(req, "kmscks").Delete(ctx, str(req.Input["CustomKeyStoreId"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ConnectCustomKeyStore", "DisconnectCustomKeyStore":
		id := str(req.Input["CustomKeyStoreId"])
		st := "CONNECTED"
		if op == "DisconnectCustomKeyStore" {
			st = "DISCONNECTED"
		}
		rec, _ := p.loadExtra(ctx, req, "kmscks", id)
		if rec == nil {
			rec = map[string]any{"CustomKeyStoreId": id}
		}
		rec["ConnectionState"] = st
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "kmscks").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateGrant":
		gid := p.deps.Rand.Hex(8)
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		kid := p.resolve(ctx, req, str(req.Input["KeyId"]))
		rec["GrantId"] = gid
		rec["KeyId"] = kid
		token := p.deps.Rand.Hex(8)
		rec["GrantToken"] = token
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "kmsgrant").Put(ctx, gid, b)
		return &spi.Response{Output: map[string]any{"GrantId": gid, "GrantToken": token}}, nil
	case "ListGrants", "ListRetirableGrants":
		kvs, _, _ := p.extraCol(req, "kmsgrant").List(ctx, "", "", 0)
		var out []any
		want := p.resolve(ctx, req, str(req.Input["KeyId"]))
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if want != "" && str(rec["KeyId"]) != want {
				continue
			}
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"Grants": out}}, nil
	case "RetireGrant", "RevokeGrant":
		_ = p.extraCol(req, "kmsgrant").Delete(ctx, str(req.Input["GrantId"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutKeyPolicy":
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		_ = p.extraCol(req, "kmspolicy").Put(ctx, id, []byte(str(req.Input["Policy"])))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetKeyPolicy":
		id := p.resolve(ctx, req, str(req.Input["KeyId"]))
		b, ok, _ := p.extraCol(req, "kmspolicy").Get(ctx, id)
		pol := "{}"
		if ok {
			pol = string(b)
		}
		return &spi.Response{Output: map[string]any{"Policy": pol}}, nil
	case "ListKeyPolicies":
		return &spi.Response{Output: map[string]any{"PolicyNames": []any{"default"}}}, nil
	case "EnableKey", "DisableKey", "EnableKeyRotation", "DisableKeyRotation",
		"ScheduleKeyDeletion", "CancelKeyDeletion", "UpdateKeyDescription", "UpdatePrimaryRegion",
		"RotateKeyOnDemand", "ImportKeyMaterial", "DeleteImportedKeyMaterial":
		return p.patchKey(ctx, req, op)
	case "GetKeyRotationStatus":
		rec, err := p.loadKey(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"KeyRotationEnabled": rec["KeyRotationEnabled"] == true}}, nil
	case "ListKeyRotations":
		return &spi.Response{Output: map[string]any{"Rotations": []any{}}}, nil
	case "GetKeyLastUsage":
		return &spi.Response{Output: map[string]any{"KeyId": p.resolve(ctx, req, str(req.Input["KeyId"])), "LastUsedDate": "2020-01-01T00:00:00Z"}}, nil
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
	case "GetPublicKey":
		return nil, &spi.Fault{Code: "UnsupportedOperationException", Message: "symmetric keys have no public key", HTTPStatus: 400, Fault: "client"}
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
	case "ReplicateKey":
		rec, err := p.loadKey(ctx, req)
		if err != nil {
			return nil, err
		}
		id := p.deps.Rand.Hex(8)
		region := first(req.Input, "ReplicaRegion")
		arn := "arn:aws:kms:" + region + ":" + req.Identity.Account + ":key/" + id
		clone := map[string]any{}
		for k, v := range rec {
			clone[k] = v
		}
		clone["KeyId"] = id
		clone["Arn"] = arn
		clone["Replica"] = true
		b, _ := json.Marshal(clone)
		_ = p.col(req).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ReplicaKeyMetadata": map[string]any{"KeyId": id, "Arn": arn, "KeyState": clone["KeyState"]}}}, nil
	case "TagResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.extraCol(req, "kmstags").Put(ctx, p.resolve(ctx, req, first(req.Input, "KeyId")), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.extraCol(req, "kmstags").Delete(ctx, p.resolve(ctx, req, first(req.Input, "KeyId")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListResourceTags":
		b, ok, _ := p.extraCol(req, "kmstags").Get(ctx, p.resolve(ctx, req, first(req.Input, "KeyId")))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	default:
		return nil, spi.NotImplemented("aws.kms", op, "emulate")
	}
}

func (p *Pack) resolve(ctx context.Context, req *spi.Request, id string) string {
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

func (p *Pack) loadKey(ctx context.Context, req *spi.Request) (map[string]any, error) {
	id := p.resolve(ctx, req, str(req.Input["KeyId"]))
	b, ok, _ := p.col(req).Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, nil
}

func (p *Pack) patchKey(ctx context.Context, req *spi.Request, op string) (*spi.Response, error) {
	rec, err := p.loadKey(ctx, req)
	if err != nil {
		return nil, err
	}
	switch op {
	case "EnableKey", "CancelKeyDeletion":
		rec["KeyState"] = "Enabled"
	case "DisableKey":
		rec["KeyState"] = "Disabled"
	case "ScheduleKeyDeletion":
		rec["KeyState"] = "PendingDeletion"
		rec["DeletionDate"] = "2020-01-31T00:00:00Z"
	case "EnableKeyRotation":
		rec["KeyRotationEnabled"] = true
	case "DisableKeyRotation":
		rec["KeyRotationEnabled"] = false
	case "RotateKeyOnDemand":
		rec["Rotated"] = true
	case "UpdateKeyDescription":
		rec["Description"] = req.Input["Description"]
	case "UpdatePrimaryRegion":
		rec["PrimaryRegion"] = req.Input["PrimaryRegion"]
	case "ImportKeyMaterial":
		if mat := blob(req.Input["EncryptedKeyMaterial"]); len(mat) == 32 {
			rec["KeyMaterial"] = base64.StdEncoding.EncodeToString(mat)
		}
		rec["Imported"] = true
	case "DeleteImportedKeyMaterial":
		rec["Imported"] = false
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req).Put(ctx, str(rec["KeyId"]), b)
	out := map[string]any{"KeyId": rec["KeyId"]}
	if op == "ScheduleKeyDeletion" {
		out["KeyState"] = rec["KeyState"]
		out["DeletionDate"] = rec["DeletionDate"]
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) hmacMsg(ctx context.Context, req *spi.Request) ([]byte, error) {
	id := p.resolve(ctx, req, str(req.Input["KeyId"]))
	mat, err := p.material(ctx, req, id)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, mat)
	mac.Write(blob(req.Input["Message"]))
	return mac.Sum(nil), nil
}

func (p *Pack) loadExtra(ctx context.Context, req *spi.Request, col, id string) (map[string]any, bool) {
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
