package terraform_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sqs"
)

func TestReadPathOpsAreImplemented(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("READ_PATH.md"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(spitest.Deps(t), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.Bundle()
	var svcID string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "## aws_s3"):
			svcID = "aws.s3"
		case strings.HasPrefix(line, "## aws_dynamodb"):
			svcID = "aws.dynamodb"
		case strings.HasPrefix(line, "## aws_sqs"):
			svcID = "aws.sqs"
		case strings.HasPrefix(line, "- "):
			op := strings.TrimPrefix(line, "- ")
			if svcID == "" {
				continue
			}
			pack, ok := reg.Resolve(svcID)
			if !ok {
				t.Fatalf("no pack %s", svcID)
			}
			if !contains(pack.Operations(), op) && cat.ServiceByID(svcID).OperationByName(op) == nil {
				t.Errorf("%s.%s missing from pack and catalog", svcID, op)
			}
			if !contains(pack.Operations(), op) {
				t.Errorf("%s.%s would 501 on terraform refresh", svcID, op)
			}
		}
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
