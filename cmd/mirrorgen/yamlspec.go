package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// isYAMLSpec reports whether a file name is a YAML serialization.
func isYAMLSpec(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml")
}

// yamlToJSON re-serializes a YAML document as JSON.
//
// It exists because YAML and JSON are two spellings of one data model, not two
// kinds of specification. Every receiver here parses JSON -- Smithy, Discovery
// and OpenAPI all decode with encoding/json and struct tags -- so the choice
// was to teach three receivers YAML or to normalize the serialization once,
// before anything asks which specification a document is. Normalizing once is
// the smaller change and it is the correct layering: `Detect` answers "is this
// an OpenAPI document", which is a question about content, and the answer must
// not depend on whether the vendor wrote braces or indentation.
//
// It is needed because DigitalOcean publishes only YAML. Their bundled
// document is 3.0 MB of OpenAPI 3.0.0 with no external references, and until
// this existed the ingest walk skipped it in silence for want of an extension.
//
// The conversion is deterministic: json.Marshal sorts a map's keys, so the same
// input always produces the same bytes and the models still reproduce
// byte-for-byte from the lock. What it must never touch is the hash: the caller
// hashes the vendor's original bytes, because those are what the lock pins.
//
// It returns a probe as well as the document, and the reason is that sorting.
// `Detect` is shown the first few kilobytes of a file, which is enough for a
// vendor's own JSON because a document tends to lead with `openapi` or
// `smithy`. Re-serializing destroys that order: DigitalOcean's converted
// document begins with a megabyte of `components.examples`, and `"openapi"`
// lands at byte 1,122,354 -- so every receiver would decline it and the walk
// would skip the one document this function exists for, in silence. The probe
// is the top-level scalars alone, which is exactly what Detect reads.
func yamlToJSON(data []byte) (doc, probe []byte, err error) {
	var parsed any
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		return nil, nil, fmt.Errorf("parse yaml: %w", err)
	}
	normalized := stringKeys(parsed)
	out, err := json.Marshal(normalized)
	if err != nil {
		return nil, nil, err
	}
	p, err := json.Marshal(scalarHead(normalized))
	if err != nil {
		return nil, nil, err
	}
	return out, p, nil
}

// scalarHead is the document's top-level scalar members and nothing else.
//
// Every receiver's Detect asks the same kind of question -- does this document
// declare `openapi: 3.x`, `smithy`, `discoveryVersion` -- and every one of
// those is a top-level scalar. Handing over only those keeps the probe a few
// dozen bytes whatever the document weighs, and keeps it independent of where
// a re-serialization happened to put them.
func scalarHead(v any) map[string]any {
	top, ok := v.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	out := map[string]any{}
	for k, val := range top {
		switch val.(type) {
		case map[string]any, []any, nil:
			continue
		}
		out[k] = val
	}
	return out
}

// stringKeys rewrites every mapping key as a string.
//
// JSON objects are keyed by strings and YAML mappings are not, so a document
// that writes `200:` for a response code -- unquoted, and therefore an integer
// -- decodes to a map JSON cannot encode. Rather than refuse such a document,
// which is valid YAML and common in hand-written OpenAPI, the key is spelled
// the way the JSON serialization of the same document would have spelled it.
func stringKeys(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = stringKeys(val)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[fmt.Sprint(k)] = stringKeys(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = stringKeys(val)
		}
		return out
	}
	return v
}
