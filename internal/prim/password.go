package prim

import (
	"fmt"
	"math"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
)

// The character classes of Secrets Manager's GetRandomPassword, as its
// documentation lists them: "By default, Secrets Manager uses uppercase and
// lowercase letters, numbers, and the following characters in passwords:
// !\"#$%&'()*+,-./:;?@[\\]^_`{|}~". IncludeSpace adds a space to what a
// password is drawn from, but not to the types RequireEachIncludedType
// requires ("at least one upper and lowercase letter, one number, and one
// punctuation"): "the password can contain space characters".
var passwordClasses = []struct{ exclude, chars string }{
	{"ExcludeLowercase", "abcdefghijklmnopqrstuvwxyz"},
	{"ExcludeUppercase", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
	{"ExcludeNumbers", "0123456789"},
	{"ExcludePunctuation", "!\"#$%&'()*+,-./:;?@[\\]^_`{|}~"},
}

// RandomPassword answers a password for GetRandomPassword's parameters,
// drawn from a generator seeded by seed, so the same seed answers the same
// password. The parameters are the request members by their model names:
// PasswordLength (1 to 4096, 32 when absent), ExcludeCharacters,
// ExcludeLowercase, ExcludeUppercase, ExcludeNumbers, ExcludePunctuation,
// IncludeSpace and RequireEachIncludedType (true when absent: "the password
// contains at least one of every character type"). It answers an error, and
// no password, for parameters no password satisfies.
func RandomPassword(params map[string]any, seed string) (string, error) {
	length := 32
	if v, ok := params["PasswordLength"]; ok && v != nil {
		n := -1.0
		switch t := v.(type) {
		case float64:
			n = t
		case int64:
			n = float64(t)
		case int:
			n = float64(t)
		}
		if n != math.Trunc(n) || n < 1 || n > 4096 {
			return "", fmt.Errorf("PasswordLength must be a whole number from 1 to 4096")
		}
		length = int(n)
	}
	on := func(member string) bool { b, _ := params[member].(bool); return b }
	excluded, _ := params["ExcludeCharacters"].(string)
	var classes []string
	for _, c := range passwordClasses {
		if !on(c.exclude) {
			classes = append(classes, c.chars)
		}
	}
	var pool []string
	for _, chars := range classes {
		if kept := strings.Map(func(r rune) rune {
			if strings.ContainsRune(excluded, r) {
				return -1
			}
			return r
		}, chars); kept != "" {
			pool = append(pool, kept)
		}
	}
	if len(pool) == 0 {
		return "", fmt.Errorf("no characters are left to make a password from")
	}
	require := true
	if v, ok := params["RequireEachIncludedType"].(bool); ok {
		require = v
	}
	if require && length < len(pool) {
		return "", fmt.Errorf("a password of %d characters cannot include each of the %d required character types", length, len(pool))
	}

	// The repository's seeded source: nothing outside clock and rand may draw
	// randomness of its own.
	rng := rand.New(seed)
	all := strings.Join(pool, "")
	if on("IncludeSpace") && !strings.Contains(excluded, " ") {
		all += " "
	}
	out := make([]byte, 0, length)
	if require {
		for _, chars := range pool {
			out = append(out, chars[rng.Intn(len(chars))])
		}
	}
	for len(out) < length {
		out = append(out, all[rng.Intn(len(all))])
	}
	for i := len(out) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		out[i], out[j] = out[j], out[i]
	}
	return string(out), nil
}

// secretsmanager.random_password answers RandomPassword for a request's
// members and a seed; secretsmanager.random_password_error answers why those
// members make no password, or "" when they do, so a bundle can refuse the
// request before it draws one.
func init() {
	params := func(name string, args []any) (map[string]any, error) {
		if len(args) == 0 {
			return nil, fmt.Errorf("%s takes the request members first", name)
		}
		m, _ := args[0].(map[string]any)
		return m, nil
	}
	Register(Func{
		Name:    "secretsmanager.random_password",
		Version: 1,
		Call: func(args []any) (any, error) {
			m, err := params("secretsmanager.random_password", args)
			if err != nil || len(args) != 2 {
				return nil, fmt.Errorf("secretsmanager.random_password takes request members and a seed")
			}
			return RandomPassword(m, fmt.Sprint(args[1]))
		},
	})
	Register(Func{
		Name:    "secretsmanager.random_password_error",
		Version: 1,
		Call: func(args []any) (any, error) {
			m, err := params("secretsmanager.random_password_error", args)
			if err != nil {
				return nil, err
			}
			if _, err := RandomPassword(m, ""); err != nil {
				return err.Error(), nil
			}
			return "", nil
		},
	})
}
