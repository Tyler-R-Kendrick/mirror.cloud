package proxy

import (
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
)

const redacted = "[SCRUBBED]"

// Scrub holds extra write-time secret patterns (regular expressions).
// Matching text is redacted when an Interaction is written to a cassette.
type Scrub struct {
	Patterns []string
}

var secretKeyFrag = []string{"SECRET", "TOKEN", "PASSWORD", "KEY", "CREDENTIAL"}

func applyScrub(i *Interaction, patterns []string) *Interaction {
	if i == nil {
		return nil
	}
	key := i.Key
	res := compilePatterns(patterns)
	secrets := harvestSecrets(i)

	i.Path = replaceSecrets(i.Path, secrets, res)
	i.Query = scrubQueryWith(i.Query, secrets, res)
	i.RequestHeaders = scrubHeader(i.RequestHeaders, secrets, res)
	i.ResponseHeaders = scrubHeader(i.ResponseHeaders, secrets, res)
	i.RequestBody = []byte(replaceSecrets(string(i.RequestBody), secrets, res))
	i.ResponseBody = []byte(replaceSecrets(string(i.ResponseBody), secrets, res))
	// Lookup identity is computed before scrub; keep it so replay matches.
	i.Key = key
	return i
}

func harvestSecrets(i *Interaction) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(v string) {
		if v == "" || v == redacted {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	for _, v := range envSecretValues() {
		add(v)
	}
	for _, h := range []http.Header{i.RequestHeaders, i.ResponseHeaders} {
		if h == nil {
			continue
		}
		for _, name := range []string{"Authorization", "X-Amz-Security-Token"} {
			for _, v := range h.Values(name) {
				add(v)
			}
		}
	}
	q, err := url.ParseQuery(i.Query)
	if err == nil {
		for k, vs := range q {
			if secretQueryParam(k) {
				for _, v := range vs {
					add(v)
				}
			}
		}
	}
	sort.Slice(out, func(a, b int) bool { return len(out[a]) > len(out[b]) })
	return out
}

func envSecretValues() []string {
	var out []string
	for _, e := range os.Environ() {
		k, v, ok := strings.Cut(e, "=")
		if !ok || len(v) <= 8 || v == redacted {
			continue
		}
		uk := strings.ToUpper(k)
		for _, frag := range secretKeyFrag {
			if strings.Contains(uk, frag) {
				out = append(out, v)
				break
			}
		}
	}
	sort.Slice(out, func(a, b int) bool { return len(out[a]) > len(out[b]) })
	return out
}

func compilePatterns(patterns []string) []*regexp.Regexp {
	var out []*regexp.Regexp
	for _, p := range patterns {
		if p == "" {
			continue
		}
		re, err := regexp.Compile(p)
		if err != nil {
			re = regexp.MustCompile(regexp.QuoteMeta(p))
		}
		out = append(out, re)
	}
	return out
}

func redactString(s string, extra []*regexp.Regexp) string {
	return replaceSecrets(s, envSecretValues(), extra)
}

func replaceSecrets(s string, secrets []string, extra []*regexp.Regexp) string {
	if s == "" {
		return s
	}
	for _, v := range secrets {
		if v == "" {
			continue
		}
		s = strings.ReplaceAll(s, v, redacted)
	}
	for _, re := range extra {
		s = re.ReplaceAllString(s, redacted)
	}
	return s
}

func secretQueryParam(k string) bool {
	return strings.EqualFold(k, "X-Amz-Security-Token") || strings.EqualFold(k, "Authorization")
}

func scrubQuery(raw string, extra []*regexp.Regexp) string {
	return scrubQueryWith(raw, envSecretValues(), extra)
}

func scrubQueryWith(raw string, secrets []string, extra []*regexp.Regexp) string {
	if raw == "" {
		return ""
	}
	v, err := url.ParseQuery(raw)
	if err != nil {
		return replaceSecrets(raw, secrets, extra)
	}
	for k, vs := range v {
		for i, val := range vs {
			if secretQueryParam(k) {
				vs[i] = redacted
				continue
			}
			vs[i] = replaceSecrets(val, secrets, extra)
		}
		v[k] = vs
	}
	return v.Encode()
}

func scrubHeader(h http.Header, secrets []string, extra []*regexp.Regexp) http.Header {
	if h == nil {
		return nil
	}
	out := h.Clone()
	for k, vs := range out {
		canon := http.CanonicalHeaderKey(k)
		for i, val := range vs {
			if canon == "Authorization" || canon == "X-Amz-Security-Token" {
				vs[i] = redacted
				continue
			}
			vs[i] = replaceSecrets(val, secrets, extra)
		}
		out[k] = vs
	}
	return out
}
