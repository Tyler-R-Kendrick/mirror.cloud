// Package cognitoidp is Cognito User Pools: password auth plus local HS256 JWTs.
package cognitoidp

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cognito-idp", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Cognito User Pools-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cognito-idp" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateUserPool", "DescribeUserPool", "ListUserPools", "DeleteUserPool",
		"AdminCreateUser", "AdminGetUser", "ListUsers", "AdminDeleteUser",
		"AdminSetUserPassword", "CreateUserPoolClient", "DescribeUserPoolClient", "ListUserPoolClients", "DeleteUserPoolClient",
		"SignUp", "ConfirmSignUp", "InitiateAuth", "AdminInitiateAuth", "GetUser", "GlobalSignOut",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateUserPool":
		id := p.deps.Rand.Hex(8)
		name := first(req.Input, "PoolName")
		rec := map[string]any{"Id": id, "Name": name, "Arn": "arn:aws:cognito-idp:" + req.Identity.Region + ":" + req.Identity.Account + ":userpool/" + req.Identity.Region + "_" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cup").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"UserPool": rec}}, nil
	case "DescribeUserPool":
		id := poolID(first(req.Input, "UserPoolId"))
		b, ok, _ := p.col(req, "cup").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"UserPool": rec}}, nil
	case "ListUserPools":
		kvs, _, _ := p.col(req, "cup").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"UserPools": items}}, nil
	case "DeleteUserPool":
		_ = p.col(req, "cup").Delete(ctx, poolID(first(req.Input, "UserPoolId")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AdminCreateUser":
		pool := poolID(first(req.Input, "UserPoolId"))
		name := first(req.Input, "Username")
		rec := map[string]any{"Username": name, "UserStatus": "FORCE_CHANGE_PASSWORD", "Enabled": true}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cu:"+pool).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "AdminGetUser":
		pool := poolID(first(req.Input, "UserPoolId"))
		name := first(req.Input, "Username")
		b, ok, _ := p.col(req, "cu:"+pool).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "UserNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListUsers":
		pool := poolID(first(req.Input, "UserPoolId"))
		kvs, _, _ := p.col(req, "cu:"+pool).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Users": items}}, nil
	case "AdminDeleteUser":
		pool := poolID(first(req.Input, "UserPoolId"))
		name := first(req.Input, "Username")
		p.dropSession(ctx, req, pool, name)
		_ = p.col(req, "cu:"+pool).Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "AdminSetUserPassword":
		pool := poolID(first(req.Input, "UserPoolId"))
		name := first(req.Input, "Username")
		b, ok, _ := p.col(req, "cu:"+pool).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "UserNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		rec["Password"] = first(req.Input, "Password")
		rec["UserStatus"] = "CONFIRMED"
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "cu:"+pool).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateUserPoolClient":
		pool := poolID(first(req.Input, "UserPoolId"))
		cid := p.deps.Rand.Hex(8)
		rec := map[string]any{"ClientId": cid, "UserPoolId": first(req.Input, "UserPoolId"), "ClientName": first(req.Input, "ClientName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cucl").Put(ctx, cid, b)
		_ = p.col(req, "cuclpool:"+pool).Put(ctx, cid, b)
		return &spi.Response{Output: map[string]any{"UserPoolClient": rec}}, nil
	case "DescribeUserPoolClient":
		cid := first(req.Input, "ClientId")
		b, ok, _ := p.col(req, "cucl").Get(ctx, cid)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"UserPoolClient": rec}}, nil
	case "ListUserPoolClients":
		pool := poolID(first(req.Input, "UserPoolId"))
		kvs, _, _ := p.col(req, "cuclpool:"+pool).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"UserPoolClients": items}}, nil
	case "DeleteUserPoolClient":
		cid := first(req.Input, "ClientId")
		b, ok, _ := p.col(req, "cucl").Get(ctx, cid)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			_ = p.col(req, "cuclpool:"+poolID(first(rec, "UserPoolId"))).Delete(ctx, cid)
		}
		_ = p.col(req, "cucl").Delete(ctx, cid)
		return &spi.Response{Output: map[string]any{}}, nil
	case "SignUp":
		cid := first(req.Input, "ClientId")
		pool, err := p.poolForClient(ctx, req, cid)
		if err != nil {
			return nil, err
		}
		name := first(req.Input, "Username")
		rec := map[string]any{"Username": name, "Password": first(req.Input, "Password"), "UserStatus": "UNCONFIRMED", "Enabled": true}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cu:"+pool).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"UserConfirmed": false, "UserSub": p.deps.Rand.Hex(8)}}, nil
	case "ConfirmSignUp":
		cid := first(req.Input, "ClientId")
		pool, err := p.poolForClient(ctx, req, cid)
		if err != nil {
			return nil, err
		}
		name := first(req.Input, "Username")
		b, ok, _ := p.col(req, "cu:"+pool).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "UserNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		rec["UserStatus"] = "CONFIRMED"
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "cu:"+pool).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "InitiateAuth", "AdminInitiateAuth":
		return p.initiate(ctx, req)
	case "GetUser":
		tok := first(req.Input, "AccessToken")
		b, ok, _ := p.col(req, "cutok").Get(ctx, tok)
		if !ok {
			return nil, &spi.Fault{Code: "NotAuthorizedException", HTTPStatus: 400, Fault: "client"}
		}
		var sess map[string]any
		_ = json.Unmarshal(b, &sess)
		ub, uok, _ := p.col(req, "cu:"+first(sess, "Pool")).Get(ctx, first(sess, "Username"))
		if !uok {
			return nil, &spi.Fault{Code: "UserNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(ub, &rec)
		return &spi.Response{Output: rec}, nil
	case "GlobalSignOut":
		tok := first(req.Input, "AccessToken")
		b, ok, _ := p.col(req, "cutok").Get(ctx, tok)
		if ok {
			var sess map[string]any
			_ = json.Unmarshal(b, &sess)
			p.dropSession(ctx, req, first(sess, "Pool"), first(sess, "Username"))
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.cognito-idp", req.Operation, "emulate")
	}
}

func (p *Pack) poolForClient(ctx context.Context, req *spi.Request, cid string) (string, error) {
	if cid == "" {
		return "", &spi.Fault{Code: "InvalidParameterException", HTTPStatus: 400, Fault: "client"}
	}
	b, ok, _ := p.col(req, "cucl").Get(ctx, cid)
	if !ok {
		return "", &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return poolID(first(rec, "UserPoolId")), nil
}

func (p *Pack) initiate(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	flow := first(req.Input, "AuthFlow")
	if flow != "" && flow != "USER_PASSWORD_AUTH" && flow != "ADMIN_USER_PASSWORD_AUTH" && flow != "ADMIN_NO_SRP_AUTH" {
		// ponytail: SRP/custom/refresh not implemented.
		return nil, &spi.Fault{Code: "InvalidParameterException", Message: "only USER_PASSWORD_AUTH and ADMIN_* password flows", HTTPStatus: 400, Fault: "client"}
	}
	params := authParams(req.Input)
	name := first(params, "USERNAME")
	pass := first(params, "PASSWORD")
	pool := poolID(first(req.Input, "UserPoolId"))
	if pool == "" {
		var err error
		pool, err = p.poolForClient(ctx, req, first(req.Input, "ClientId"))
		if err != nil {
			return nil, err
		}
	}
	b, ok, _ := p.col(req, "cu:"+pool).Get(ctx, name)
	if !ok {
		return nil, &spi.Fault{Code: "UserNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	if first(rec, "UserStatus") == "UNCONFIRMED" {
		return nil, &spi.Fault{Code: "UserNotConfirmedException", HTTPStatus: 400, Fault: "client"}
	}
	if first(rec, "Password") == "" || first(rec, "Password") != pass {
		return nil, &spi.Fault{Code: "NotAuthorizedException", HTTPStatus: 400, Fault: "client"}
	}
	cid := first(req.Input, "ClientId")
	access, idt := p.accessJWT(req, pool, cid, name), p.idJWT(req, pool, cid, name)
	refresh := p.deps.Rand.Hex(16)
	sess := map[string]any{"Username": name, "Pool": pool, "RefreshToken": refresh}
	sb, _ := json.Marshal(sess)
	_ = p.col(req, "cutok").Put(ctx, access, sb)
	_ = p.col(req, "cusess:"+pool).Put(ctx, name, []byte(access))
	return &spi.Response{Output: map[string]any{"AuthenticationResult": map[string]any{
		"AccessToken": access, "IdToken": idt, "RefreshToken": refresh, "ExpiresIn": 3600, "TokenType": "Bearer",
	}}}, nil
}

func (p *Pack) dropSession(ctx context.Context, req *spi.Request, pool, name string) {
	b, ok, _ := p.col(req, "cusess:"+pool).Get(ctx, name)
	if ok {
		_ = p.col(req, "cutok").Delete(ctx, string(b))
	}
	_ = p.col(req, "cusess:"+pool).Delete(ctx, name)
}

func authParams(in map[string]any) map[string]any {
	if m, ok := in["AuthParameters"].(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func poolID(s string) string {
	if i := lastUnderscore(s); i >= 0 {
		return s[i+1:]
	}
	return s
}

func lastUnderscore(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '_' {
			return i
		}
	}
	return -1
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
