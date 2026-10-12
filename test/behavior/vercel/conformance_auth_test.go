package vercel

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func init() { registerArea("api-keys-and-oauth", (*conf).apiKeysAndOAuth) }

// apiKeysAndOAuth drives the authored routes the vendored document does not
// declare: the API-key lifecycle, the registration probe and the local OAuth
// flow. They are held to the supplement's schemas, which are authored, and the
// ledger names the two that answer an HTML page and a redirect.
func (c *conf) apiKeysAndOAuth(t *testing.T) {
	c.call(t, http.MethodGet, "/registration", nil, 200)

	key := c.call(t, http.MethodPost, "/v1/api-keys", map[string]any{"name": "ci"}, 200)
	created, _ := key.body["apiKey"].(map[string]any)
	kid := str(created, "id")
	c.call(t, http.MethodGet, "/v1/api-keys", nil, 200)
	c.call(t, http.MethodDelete, "/v1/api-keys/"+kid, nil, 200)
	c.call(t, http.MethodDelete, "/v1/api-keys/key_nope", nil, 404)

	// The OAuth leg that needs a browser is driven outside call(): its answers
	// are an HTML page and a 302, which no JSON schema describes.
	form := url.Values{"username": {"test"}, "redirect_uri": {"http://localhost:3000/cb"}, "state": {"s9"}, "client_id": {"client_x"}}
	req, err := http.NewRequest(http.MethodPost, c.ts.URL+"/oauth/authorize/callback", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "api.vercel.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	loc := res.Header.Get("Location")
	if res.StatusCode != http.StatusFound || !strings.Contains(loc, "code=") {
		t.Fatalf("the callback answered %d with Location %q", res.StatusCode, loc)
	}
	code := strings.Split(strings.Split(loc, "code=")[1], "&")[0]

	token := url.Values{"code": {code}, "redirect_uri": {"http://localhost:3000/cb"}, "client_id": {"client_x"}}
	tok := c.call(t, http.MethodPost, "/login/oauth/token", token.Encode(), 200, "Content-Type", "application/x-www-form-urlencoded")
	c.call(t, http.MethodPost, "/login/oauth/token", token.Encode(), 400, "Content-Type", "application/x-www-form-urlencoded")
	c.call(t, http.MethodGet, "/login/oauth/userinfo", nil, 200, "Authorization", "Bearer "+str(tok.body, "access_token"))
	c.call(t, http.MethodGet, "/login/oauth/userinfo", nil, 401, "Authorization", "")
}
