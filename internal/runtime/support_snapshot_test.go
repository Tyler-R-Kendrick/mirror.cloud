package runtime

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/allservices"
)

func TestSupportMatrixSnapshot(t *testing.T) {
	needEveryService(t)
	golden.Assert(t, []byte(SupportMatrix()))
}
