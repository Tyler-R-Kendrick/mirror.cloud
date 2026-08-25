// Package eventhttp invokes HTTP endpoints with stored EventBridge connection credentials.
package eventhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Call describes one connection-authenticated HTTP invocation.
type Call struct {
	Endpoint, Method, UserAgent, Range string
	Headers, Query                     map[string]any
	Body                               []byte
	Timeout                            time.Duration
	MaxRequestBytes, MaxResponseBytes  int64
	RevealSecrets                      bool
}

// Result contains the raw response and TestState-compatible request/response inspection data.
type Result struct {
	Body       []byte
	StatusCode int
	Status     string
	Headers    http.Header
	Inspection map[string]any
}

// Connection loads an EventBridge connection by ARN.
func Connection(ctx context.Context, deps spi.Deps, identity spi.Identity, arn string) (map[string]any, bool) {
	_, resource, found := strings.Cut(arn, "connection/")
	name, _, _ := strings.Cut(resource, "/")
	if !found || name == "" {
		return nil, false
	}
	body, ok, _ := deps.Store.Scope(identity.Account, identity.Region).Collection("connections").Get(ctx, name)
	var connection map[string]any
	if !ok || json.Unmarshal(body, &connection) != nil {
		return nil, false
	}
	return connection, true
}

// Invoke performs a call using an EventBridge connection.
func Invoke(ctx context.Context, connection map[string]any, call Call) (Result, error) {
	if connection["ConnectionState"] != "AUTHORIZED" {
		return Result{}, &spi.Fault{Code: "ConnectionFailure", Message: "Connection is not authorized.", HTTPStatus: 400, Fault: "client"}
	}
	auth, _ := connection["AuthParameters"].(map[string]any)
	invocation, _ := auth["InvocationHttpParameters"].(map[string]any)
	body, err := MergeBody(call.Body, invocation["BodyParameters"], call.MaxRequestBytes)
	if err != nil {
		return Result{}, err
	}
	request, err := http.NewRequestWithContext(ctx, call.Method, call.Endpoint, bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	applyMap(request.Header, call.Headers)
	query := request.URL.Query()
	applyMap(query, call.Query)
	applyParameters(request.Header.Set, invocation["HeaderParameters"])
	applyParameters(query.Set, invocation["QueryStringParameters"])
	request.URL.RawQuery = query.Encode()
	if call.UserAgent != "" {
		request.Header.Set("User-Agent", call.UserAgent)
	}
	if call.Range != "" {
		request.Header.Set("Range", call.Range)
	}
	client := &http.Client{Timeout: call.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	authorizationType := stringValue(connection["AuthorizationType"])
	if err := authorize(ctx, client, request, authorizationType, auth); err != nil {
		return Result{}, err
	}
	requestTrace := traceRequest(request, body, invocation, auth, authorizationType, call.RevealSecrets)
	response, err := client.Do(request)
	if err != nil {
		return Result{Inspection: map[string]any{"request": requestTrace}}, err
	}
	if authorizationType == "OAUTH_CLIENT_CREDENTIALS" && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusProxyAuthRequired) {
		response.Body.Close()
		if err := authorize(ctx, client, request, authorizationType, auth); err != nil {
			return Result{Inspection: map[string]any{"request": requestTrace}}, err
		}
		request.Body, _ = request.GetBody()
		response, err = client.Do(request)
		if err != nil {
			return Result{Inspection: map[string]any{"request": requestTrace}}, err
		}
	}
	defer response.Body.Close()
	limit := call.MaxResponseBytes
	if limit <= 0 {
		limit = 6 << 20
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	result := Result{Body: responseBody, StatusCode: response.StatusCode, Status: response.Status, Headers: response.Header.Clone()}
	result.Inspection = map[string]any{"request": requestTrace, "response": traceResponse(response, responseBody)}
	if err != nil {
		return result, err
	}
	if int64(len(responseBody)) > limit {
		return result, &spi.Fault{Code: "TargetInvocationFailed", Message: "HTTP response exceeds its maximum size.", HTTPStatus: 502, Fault: "server"}
	}
	return result, nil
}

// MergeBody applies connection body parameters, which override request values.
func MergeBody(body []byte, raw any, limit int64) ([]byte, error) {
	parameters := sliceValue(raw)
	if len(parameters) == 0 {
		return body, nil
	}
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	var object map[string]any
	if json.Unmarshal(body, &object) != nil || object == nil {
		return nil, &spi.Fault{Code: "TargetInvocationFailed", Message: "Connection body parameters require a JSON object payload.", HTTPStatus: 400, Fault: "client"}
	}
	applyParameters(func(key, value string) { object[key] = value }, parameters)
	merged, _ := json.Marshal(object)
	if limit > 0 && int64(len(merged)) > limit {
		return nil, &spi.Fault{Code: "TargetInvocationFailed", Message: "HTTP request exceeds its maximum size.", HTTPStatus: 400, Fault: "client"}
	}
	return merged, nil
}

func authorize(ctx context.Context, client *http.Client, request *http.Request, authorizationType string, auth map[string]any) error {
	switch authorizationType {
	case "API_KEY":
		apiKey, _ := auth["ApiKeyAuthParameters"].(map[string]any)
		request.Header.Set(stringValue(apiKey["ApiKeyName"]), stringValue(apiKey["ApiKeyValue"]))
	case "BASIC":
		basic, _ := auth["BasicAuthParameters"].(map[string]any)
		request.SetBasicAuth(stringValue(basic["Username"]), stringValue(basic["Password"]))
	case "OAUTH_CLIENT_CREDENTIALS":
		token, err := oauthToken(ctx, client, auth)
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", token)
	default:
		return &spi.Fault{Code: "ValidationException", Message: "Unsupported connection authorization type.", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func oauthToken(ctx context.Context, client *http.Client, auth map[string]any) (string, error) {
	oauth, _ := auth["OAuthParameters"].(map[string]any)
	parameters, _ := oauth["OAuthHttpParameters"].(map[string]any)
	form := url.Values{}
	applyParameters(form.Set, parameters["BodyParameters"])
	request, err := http.NewRequestWithContext(ctx, stringValue(oauth["HttpMethod"]), stringValue(oauth["AuthorizationEndpoint"]), strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	applyParameters(request.Header.Set, parameters["HeaderParameters"])
	query := request.URL.Query()
	applyParameters(query.Set, parameters["QueryStringParameters"])
	request.URL.RawQuery = query.Encode()
	credentials, _ := oauth["ClientParameters"].(map[string]any)
	request.SetBasicAuth(stringValue(credentials["ClientID"]), stringValue(credentials["ClientSecret"]))
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var token map[string]any
	if response.StatusCode < 200 || response.StatusCode >= 300 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&token) != nil || stringValue(token["access_token"]) == "" {
		return "", &spi.Fault{Code: "ConnectionFailure", Message: "OAuth token request failed.", HTTPStatus: 502, Fault: "server"}
	}
	tokenType := stringValue(token["token_type"])
	if tokenType == "" {
		tokenType = "Bearer"
	}
	// ponytail: fetch per invocation; cache by connection when throughput makes token calls material.
	return tokenType + " " + stringValue(token["access_token"]), nil
}

type values interface {
	Add(string, string)
	Set(string, string)
}

func applyMap(target values, raw map[string]any) {
	for key, rawValue := range raw {
		switch value := rawValue.(type) {
		case []any:
			for _, item := range value {
				target.Add(key, stringValue(item))
			}
		default:
			target.Set(key, stringValue(value))
		}
	}
}

func applyParameters(set func(string, string), raw any) {
	for _, value := range sliceValue(raw) {
		parameter, _ := value.(map[string]any)
		set(stringValue(parameter["Key"]), stringValue(parameter["Value"]))
	}
}

func traceRequest(request *http.Request, body []byte, invocation, auth map[string]any, authorizationType string, reveal bool) map[string]any {
	headers := request.Header.Clone()
	traceURL := request.URL.String()
	traceBody := body
	if !reveal {
		for _, raw := range sliceValue(invocation["HeaderParameters"]) {
			parameter, _ := raw.(map[string]any)
			headers.Del(stringValue(parameter["Key"]))
		}
		query := request.URL.Query()
		for _, raw := range sliceValue(invocation["QueryStringParameters"]) {
			parameter, _ := raw.(map[string]any)
			query.Del(stringValue(parameter["Key"]))
		}
		if len(query) != len(request.URL.Query()) {
			copy := *request.URL
			copy.RawQuery = query.Encode()
			traceURL = copy.String()
		}
		if authorizationType == "API_KEY" {
			apiKey, _ := auth["ApiKeyAuthParameters"].(map[string]any)
			headers.Del(stringValue(apiKey["ApiKeyName"]))
		} else {
			headers.Del("Authorization")
		}
		if parameters := sliceValue(invocation["BodyParameters"]); len(parameters) != 0 {
			var object map[string]any
			if json.Unmarshal(body, &object) == nil {
				for _, raw := range parameters {
					parameter, _ := raw.(map[string]any)
					delete(object, stringValue(parameter["Key"]))
				}
				traceBody, _ = json.Marshal(object)
			}
		}
	}
	encodedHeaders, _ := json.Marshal(headers)
	return map[string]any{"protocol": request.Proto, "method": request.Method, "url": traceURL, "headers": string(encodedHeaders), "body": string(traceBody)}
}

func traceResponse(response *http.Response, body []byte) map[string]any {
	headers, _ := json.Marshal(response.Header)
	statusMessage := strings.TrimSpace(strings.TrimPrefix(response.Status, strconv.Itoa(response.StatusCode)))
	return map[string]any{
		"protocol": response.Proto, "statusCode": strconv.Itoa(response.StatusCode), "statusMessage": statusMessage,
		"headers": string(headers), "body": string(body),
	}
}

func sliceValue(value any) []any { values, _ := value.([]any); return values }

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
