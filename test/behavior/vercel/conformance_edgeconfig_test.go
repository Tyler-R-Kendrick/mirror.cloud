package vercel

import (
	"net/http"
	"testing"
)

func init() { registerArea("edge-config", (*conf).edgeConfig) }

// edgeConfig drives a config through its life at both spellings of its route:
// created, filled, read, tokened, given a schema, renamed and deleted.
func (c *conf) edgeConfig(t *testing.T) {
	made := c.call(t, http.MethodPost, "/v1/global-config", map[string]any{"slug": "conform_cfg", "items": map[string]any{"greeting": "hello", "flags": map[string]any{"beta": true}}}, 201)
	id := str(made.body, "id")
	c.call(t, http.MethodPost, "/v1/edge-config", map[string]any{"slug": "second"}, 201)
	c.call(t, http.MethodPost, "/v1/edge-config", map[string]any{"slug": "1-bad"}, 400)

	c.call(t, http.MethodGet, "/v1/global-config", nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config", nil, 200)
	c.call(t, http.MethodGet, "/v1/global-config/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id, nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/ecfg_nope", nil, 404)
	c.call(t, http.MethodPut, "/v1/global-config/"+id, map[string]any{"slug": "conform_renamed"}, 200)
	c.call(t, http.MethodPut, "/v1/edge-config/"+id, map[string]any{"slug": "conform_cfg"}, 200)
	c.call(t, http.MethodPut, "/v1/edge-config/ecfg_nope", map[string]any{"slug": "x"}, 404)

	// Items.
	c.call(t, http.MethodPatch, "/v1/global-config/"+id+"/items", map[string]any{"items": []map[string]any{
		{"operation": "create", "key": "count", "value": 3, "description": "a number"},
		{"operation": "upsert", "key": "greeting", "value": "hi"},
	}}, 200)
	c.call(t, http.MethodPatch, "/v1/edge-config/"+id+"/items", map[string]any{"items": []map[string]any{
		{"operation": "update", "key": "count", "description": "still a number"},
		{"operation": "delete", "key": "flags"},
	}}, 200)
	c.call(t, http.MethodPatch, "/v1/edge-config/"+id+"/items", map[string]any{"items": []map[string]any{{"operation": "create", "key": "count", "value": 1}}}, 409)
	c.call(t, http.MethodPatch, "/v1/edge-config/"+id+"/items", map[string]any{"items": []map[string]any{{"operation": "delete", "key": "ghost"}}}, 404)
	c.call(t, http.MethodGet, "/v1/global-config/"+id+"/items", nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/items", nil, 200)
	c.call(t, http.MethodGet, "/v1/global-config/"+id+"/item/greeting", nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/item/count", nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/item/ghost", nil, 404)

	// Tokens.
	tok := c.call(t, http.MethodPost, "/v1/global-config/"+id+"/token", map[string]any{"label": "reader"}, 201)
	token, tid := str(tok.body, "token"), str(tok.body, "id")
	other := c.call(t, http.MethodPost, "/v1/edge-config/"+id+"/token", map[string]any{"label": "writer"}, 201)
	c.call(t, http.MethodPost, "/v1/edge-config/"+id+"/token", map[string]any{"label": ""}, 400)
	c.call(t, http.MethodGet, "/v1/global-config/"+id+"/tokens", nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/tokens", nil, 200)
	c.call(t, http.MethodGet, "/v1/global-config/"+id+"/token/"+token, nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/token/"+token, nil, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/token/nope", nil, 404)
	c.call(t, http.MethodDelete, "/v1/global-config/"+id+"/tokens", map[string]any{"ids": []string{tid}}, 204)
	c.call(t, http.MethodDelete, "/v1/edge-config/"+id+"/tokens", map[string]any{"tokens": []string{str(other.body, "token")}}, 204)
	c.call(t, http.MethodDelete, "/v1/edge-config/"+id+"/tokens", map[string]any{}, 400)

	// Schema.
	c.call(t, http.MethodGet, "/v1/global-config/"+id+"/schema", nil, 200)
	c.call(t, http.MethodPost, "/v1/global-config/"+id+"/schema", map[string]any{"definition": map[string]any{"type": "object"}}, 200)
	c.call(t, http.MethodPost, "/v1/edge-config/"+id+"/schema?dryRun=true", map[string]any{"definition": map[string]any{"type": "object", "required": []string{"greeting"}}}, 200)
	c.call(t, http.MethodGet, "/v1/edge-config/"+id+"/schema", nil, 200)
	c.call(t, http.MethodPost, "/v1/edge-config/"+id+"/schema", map[string]any{"definition": "nope"}, 400)
	c.call(t, http.MethodDelete, "/v1/global-config/"+id+"/schema", nil, 204)
	c.call(t, http.MethodDelete, "/v1/edge-config/"+id+"/schema", nil, 204)

	// Deleted at each spelling.
	c.call(t, http.MethodDelete, "/v1/global-config/"+id, nil, 204)
	c.call(t, http.MethodDelete, "/v1/edge-config/"+id, nil, 404)
}
