package runtime

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/secretsmanager"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sns"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sqs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/ssm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sts"
)

func TestSupportMatrixSnapshot(t *testing.T) {
	golden.Assert(t, []byte(SupportMatrix()))
}
