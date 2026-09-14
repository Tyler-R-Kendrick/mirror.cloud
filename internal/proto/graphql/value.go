package graphql

import (
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// stringValue reads a string literal into its value, decoding escapes.
//
// Escapes are decoded rather than skipped because an argument's value is
// compared and stored, not re-emitted: a bucket named `a"b` written as `a\"b`
// must arrive as the three characters it is. The previous reader only needed to
// find the closing quote, which is why it could treat a backslash as "skip one"
// and stop there.
func (s *scanner) stringValue() (any, bool) {
	if s.i+2 < s.n && s.q[s.i:s.i+3] == `"""` {
		return s.blockStringValue()
	}
	s.i++ // opening quote
	var b strings.Builder
	for s.i < s.n {
		c := s.q[s.i]
		switch c {
		case '"':
			s.i++
			return b.String(), true
		case '\\':
			s.i++
			if s.i >= s.n {
				return nil, false
			}
			switch e := s.q[s.i]; e {
			case '"', '\\', '/':
				b.WriteByte(e)
				s.i++
			case 'b':
				b.WriteByte('\b')
				s.i++
			case 'f':
				b.WriteByte('\f')
				s.i++
			case 'n':
				b.WriteByte('\n')
				s.i++
			case 'r':
				b.WriteByte('\r')
				s.i++
			case 't':
				b.WriteByte('\t')
				s.i++
			case 'u':
				r, ok := s.unicodeEscape()
				if !ok {
					return nil, false
				}
				b.WriteRune(r)
			default:
				// Not an escape the grammar defines. Taking the character
				// literally keeps a malformed document from swallowing the
				// rest of the string looking for a quote that is right there.
				b.WriteByte(e)
				s.i++
			}
		default:
			b.WriteByte(c)
			s.i++
		}
	}
	return nil, false // unterminated
}

// unicodeEscape decodes `\uXXXX`, with the scanner on the `u`, pairing a
// surrogate with the `\uXXXX` that follows it so an astral character arrives
// whole rather than as two halves that are not characters.
func (s *scanner) unicodeEscape() (rune, bool) {
	s.i++ // past 'u'
	if s.i+4 > s.n {
		return 0, false
	}
	v, err := strconv.ParseUint(s.q[s.i:s.i+4], 16, 32)
	if err != nil {
		return 0, false
	}
	s.i += 4
	r := rune(v)
	if !utf16.IsSurrogate(r) {
		return r, true
	}
	if s.i+6 <= s.n && s.q[s.i] == '\\' && s.q[s.i+1] == 'u' {
		if lo, err := strconv.ParseUint(s.q[s.i+2:s.i+6], 16, 32); err == nil {
			if dec := utf16.DecodeRune(r, rune(lo)); dec != utf8.RuneError {
				s.i += 6
				return dec, true
			}
		}
	}
	return utf8.RuneError, true
}

// blockStringValue reads `"""..."""`, whose only escape is `\"""` and whose
// common indentation the grammar strips.
func (s *scanner) blockStringValue() (any, bool) {
	s.i += 3
	var b strings.Builder
	for s.i < s.n {
		if s.i+2 < s.n && s.q[s.i:s.i+3] == `"""` {
			s.i += 3
			return trimBlockString(b.String()), true
		}
		if s.i+3 < s.n && s.q[s.i:s.i+4] == `\"""` {
			b.WriteString(`"""`)
			s.i += 4
			continue
		}
		b.WriteByte(s.q[s.i])
		s.i++
	}
	return nil, false // unterminated
}

// trimBlockString applies the grammar's block-string value: drop leading and
// trailing blank lines, and remove the common indentation of every line after
// the first.
func trimBlockString(raw string) string {
	lines := strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n")
	common := -1
	for _, line := range lines[min(1, len(lines)):] {
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent == len(line) { // blank: says nothing about the common indent
			continue
		}
		if common < 0 || indent < common {
			common = indent
		}
	}
	if common > 0 {
		for i := 1; i < len(lines); i++ {
			if len(lines[i]) >= common {
				lines[i] = lines[i][common:]
			} else {
				lines[i] = strings.TrimLeft(lines[i], " \t")
			}
		}
	}
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return strings.Join(lines, "\n")
}

// numberValue reads an Int or a Float.
//
// The two stay distinct because the model does: an Int member and a Float
// member are different shapes, and handing a whole number to both as a float64
// is how `3` reaches a validator as `3.0` and an id round-trips as `1e+06`.
func (s *scanner) numberValue() (any, bool) {
	start := s.i
	if s.i < s.n && (s.q[s.i] == '-' || s.q[s.i] == '+') {
		s.i++
	}
	float := false
	for s.i < s.n {
		c := s.q[s.i]
		switch {
		case c >= '0' && c <= '9':
			s.i++
		case c == '.' || c == 'e' || c == 'E':
			float = true
			s.i++
		case (c == '-' || c == '+') && (s.q[s.i-1] == 'e' || s.q[s.i-1] == 'E'):
			s.i++
		default:
			goto done
		}
	}
done:
	text := s.q[start:s.i]
	if text == "" || text == "-" || text == "+" {
		return nil, false
	}
	if !float {
		if v, err := strconv.ParseInt(text, 10, 64); err == nil {
			return v, true
		}
	}
	v, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, false
	}
	return v, true
}
