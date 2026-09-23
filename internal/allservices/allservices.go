// Package allservices imports every registered service so a binary that links
// it serves the full surface.
//
// Two kinds of service land in the registry and this is the one place that
// knows both: the hand-written packs under internal/services, and the
// data-defined services in internal/bundled, which registers one engine per
// Behavior IR bundle under behavior/. Extraction moves a service from the
// first list to the second, and the only edit is deleting a line here.
//
// Keeping the list in a package rather than in main means the binary and the
// tests that assert over the registered set cannot disagree about which
// services exist.
package allservices

import (
	// Data-defined services. This import is not a list: it registers every
	// bundle under behavior/, so adding a service adds no Go anywhere.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"

	// CI/CD AfterInvoke executors (gated by MIRROR_CICD_EXECUTE=1).
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/register"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigateway"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/athena"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudcontrol"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudformation"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticloadbalancing"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/firehose"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/pipes"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/rds"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/scheduler"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcp/gcs"
)
