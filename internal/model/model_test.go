package model

import (
	"os"
	"strings"
	"testing"
)

func TestNoServiceLiterals(t *testing.T) {
	b, err := os.ReadFile("model.go")
	if err != nil {
		t.Fatal(err)
	}
	src := stripComments(string(b))
	for _, n := range []string{`"s3"`, `"dynamodb"`, `"sqs"`, `"sns"`, `"iam"`, `"sts"`} {
		if strings.Contains(src, n) {
			t.Errorf("service literal %s in model.go", n)
		}
	}
}

func TestLookups(t *testing.T) {
	op := Operation{Name: "Get"}
	service := Service{ID: "example", Operations: []Operation{op}}
	bundle := Bundle{Services: []Service{service}}
	if got := bundle.ServiceByID("example"); got == nil || got.OperationByName("Get") == nil {
		t.Fatal("lookup missed existing service or operation")
	}
	if bundle.ServiceByID("missing") != nil || (*Bundle)(nil).ServiceByID("example") != nil || service.OperationByName("missing") != nil || (*Service)(nil).OperationByName("Get") != nil {
		t.Fatal("lookup found missing service or operation")
	}
}

func stripComments(s string) string {
	var out strings.Builder
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '/' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
			i += 2
			for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
				i++
			}
			i += 2
			continue
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}
