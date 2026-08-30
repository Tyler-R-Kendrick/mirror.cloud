package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/logging"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

const websiteHostID = "h6t23Wl2Ndijztq+COn9kvx32omFVRLLtwk36D6+2/CIYSey+Uox6kBxRgcnAASsgnGwctU6zzU="

func websiteBucketHost(host string) (string, bool) {
	if index := strings.IndexByte(host, ':'); index >= 0 {
		host = host[:index]
	}
	lower := strings.ToLower(host)
	for _, marker := range []string{".s3-website.", ".s3-website-"} {
		if index := strings.Index(lower, marker); index > 0 {
			return host[:index], true
		}
	}
	return "", false
}

func websiteRequest(req *spi.Request) bool {
	if req.HTTP == nil {
		return false
	}
	_, ok := websiteBucketHost(req.HTTP.Host)
	return ok
}

func (p *Pack) websiteObject(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP.Method != http.MethodGet {
		return websiteHTML(http.StatusMethodNotAllowed, fmt.Sprintf(`<html>
<head><title>405 Method Not Allowed</title></head>
<body>
<h1>405 Method Not Allowed</h1>
<ul>
<li>Code: MethodNotAllowed</li>
<li>Message: The specified method is not allowed against this resource.</li>
<li>Method: %s</li>
<li>ResourceType: OBJECT</li>
<li>RequestId: %s</li>
<li>HostId: %s</li>
</ul>
<hr/>
</body>
</html>
`, html.EscapeString(strings.ToUpper(req.HTTP.Method)), html.EscapeString(logging.RequestID(ctx)), websiteHostID)), nil
	}

	bucket := str(req.Input["Bucket"])
	if err := p.requireBucket(ctx, req, bucket); err != nil {
		if fault, ok := err.(*spi.Fault); ok {
			return website404(ctx, fault.Code, fault.Message, bucket, ""), nil
		}
		return nil, err
	}
	raw, exists, err := p.col(req, "bktcfg").Get(ctx, bucket+"/website")
	if err != nil {
		return nil, err
	}
	if !exists {
		return website404(ctx, "NoSuchWebsiteConfiguration", "The specified bucket does not have a website configuration", bucket, ""), nil
	}
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, err
	}
	configuration := asMap(stored["WebsiteConfiguration"])
	if redirect := asMap(configuration["RedirectAllRequestsTo"]); len(redirect) != 0 {
		return websiteRedirect(req.HTTP, str(redirect["HostName"]), str(redirect["Protocol"]), "", "", http.StatusMovedPermanently), nil
	}

	key := str(req.Input["Key"])
	rules := asSlice(configuration["RoutingRules"])
	if key != "" {
		if rule := matchingWebsiteRule(rules, key, 0); rule != nil {
			return websiteRuleRedirect(req.HTTP, key, rule), nil
		}
	}
	isFolder := strings.HasSuffix(req.HTTP.URL.Path, "/")
	if key == "" || isFolder {
		key += str(asMap(configuration["IndexDocument"])["Suffix"])
	}
	response, err := p.websiteRawObject(ctx, req, bucket, key)
	if faultCode(err) == "NoSuchKey" {
		if !isFolder {
			index := strings.TrimSuffix(key, "/") + "/" + str(asMap(configuration["IndexDocument"])["Suffix"])
			if indexed, indexedErr := p.websiteRawObject(ctx, req, bucket, index); indexedErr == nil {
				if indexed.Stream != nil {
					_ = indexed.Stream.Close()
				}
				return &spi.Response{Status: http.StatusFound, Headers: http.Header{"Location": []string{"/" + strings.TrimSuffix(key, "/") + "/"}}}, nil
			}
		}
		if rule := matchingWebsiteRule(rules, key, http.StatusNotFound); rule != nil {
			return websiteRuleRedirect(req.HTTP, key, rule), nil
		}
		if errorKey := str(asMap(configuration["ErrorDocument"])["Key"]); errorKey != "" {
			errorResponse, errorErr := p.websiteRawObject(ctx, req, bucket, errorKey)
			if errorErr == nil {
				if location := errorResponse.Headers.Get("x-amz-website-redirect-location"); location != "" {
					if errorResponse.Stream != nil {
						_ = errorResponse.Stream.Close()
					}
					return &spi.Response{Status: http.StatusMovedPermanently, Headers: http.Header{"Location": []string{location}}}, nil
				}
				errorResponse.Status = http.StatusNotFound
				errorResponse.Headers = websiteObjectHeaders(errorResponse.Headers)
				return errorResponse, nil
			}
			return website404(ctx, "NoSuchKey", "The specified key does not exist.", key, errorKey), nil
		}
		return website404(ctx, "NoSuchKey", "The specified key does not exist.", key, ""), nil
	}
	if err != nil {
		return websiteHTML(http.StatusInternalServerError, "<html><head><title>500 Service Error</title></head><body><h1>500 Service Error</h1><hr/></body></html>\n"), nil
	}
	if location := response.Headers.Get("x-amz-website-redirect-location"); location != "" {
		if response.Stream != nil {
			_ = response.Stream.Close()
		}
		return &spi.Response{Status: http.StatusMovedPermanently, Headers: http.Header{"Location": []string{location}}}, nil
	}
	response.Headers = websiteObjectHeaders(response.Headers)
	return response, nil
}

func (p *Pack) websiteRawObject(ctx context.Context, req *spi.Request, bucket, key string) (*spi.Response, error) {
	child := *req
	child.Operation = "GetObject"
	child.Input = map[string]any{"Bucket": bucket, "Key": key, "_websiteRaw": true}
	return p.getObject(ctx, &child)
}

func matchingWebsiteRule(rules []any, key string, errorCode int) map[string]any {
	for _, value := range rules {
		rule := asMap(value)
		condition := asMap(rule["Condition"])
		if len(condition) == 0 {
			return rule
		}
		prefix := str(condition["KeyPrefixEquals"])
		code := str(condition["HttpErrorCodeReturnedEquals"])
		if prefix != "" && code != "" {
			if strings.HasPrefix(key, prefix) && code == strconv.Itoa(errorCode) {
				return rule
			}
			continue
		}
		if prefix != "" && strings.HasPrefix(key, prefix) || code != "" && code == strconv.Itoa(errorCode) {
			return rule
		}
	}
	return nil
}

func websiteRuleRedirect(request *http.Request, key string, rule map[string]any) *spi.Response {
	redirect := asMap(rule["Redirect"])
	status, _ := strconv.Atoi(str(redirect["HttpRedirectCode"]))
	if status == 0 {
		status = http.StatusMovedPermanently
	}
	replace, replacement := "", ""
	if value := str(redirect["ReplaceKeyWith"]); value != "" {
		replace, replacement = key, value
	} else if _, exists := redirect["ReplaceKeyPrefixWith"]; exists {
		replace = str(asMap(rule["Condition"])["KeyPrefixEquals"])
		replacement = str(redirect["ReplaceKeyPrefixWith"])
	}
	return websiteRedirect(request, str(redirect["HostName"]), str(redirect["Protocol"]), replace, replacement, status)
}

func websiteRedirect(request *http.Request, host, protocol, replace, replacement string, status int) *spi.Response {
	location := *request.URL
	location.Scheme = request.URL.Scheme
	if location.Scheme == "" {
		location.Scheme = "http"
		if request.TLS != nil {
			location.Scheme = "https"
		}
	}
	location.Host = request.Host
	if host != "" {
		location.Host = host
	}
	if protocol != "" {
		location.Scheme = protocol
	}
	if replace != "" || replacement != "" {
		key := strings.TrimPrefix(location.Path, "/")
		if replace == key {
			key = replacement
		} else {
			key = strings.Replace(key, replace, replacement, 1)
		}
		location.Path = "/" + key
	}
	return &spi.Response{Status: status, Headers: http.Header{"Location": []string{location.String()}}}
}

func websiteObjectHeaders(source http.Header) http.Header {
	headers := http.Header{}
	for _, key := range []string{"Content-Type", "ETag"} {
		if value := source.Get(key); value != "" {
			headers.Set(key, value)
		}
	}
	return headers
}

func website404(ctx context.Context, code, message, resource, errorDocument string) *spi.Response {
	resourceName := "BucketName"
	if strings.Contains(code, "Key") {
		resourceName = "Key"
	}
	nested := ""
	if errorDocument != "" {
		nested = fmt.Sprintf(`<h3>An Error Occurred While Attempting to Retrieve a Custom Error Document</h3>
<ul>
<li>Code: NoSuchKey</li>
<li>Message: The specified key does not exist.</li>
<li>Key: %s</li>
</ul>
`, html.EscapeString(errorDocument))
	}
	body := fmt.Sprintf(`<html>
<head><title>404 Not Found</title></head>
<body>
<h1>404 Not Found</h1>
<ul>
<li>Code: %s</li>
<li>Message: %s</li>
<li>%s: %s</li>
<li>RequestId: %s</li>
<li>HostId: %s</li>
</ul>
%s<hr/>
</body>
</html>
`, html.EscapeString(code), html.EscapeString(message), resourceName, html.EscapeString(resource), html.EscapeString(logging.RequestID(ctx)), websiteHostID, nested)
	return websiteHTML(http.StatusNotFound, body)
}

func websiteHTML(status int, body string) *spi.Response {
	return &spi.Response{Status: status, Headers: http.Header{"Content-Type": []string{"text/html; charset=utf-8"}}, Stream: io.NopCloser(strings.NewReader(body))}
}

func faultCode(err error) string {
	if fault, ok := err.(*spi.Fault); ok {
		return fault.Code
	}
	return ""
}
