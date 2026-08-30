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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/acm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/amplify"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigateway"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigatewayv2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/appconfig"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/appsync"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/athena"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/autoscaling"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudcontrol"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudformation"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudfront"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codebuild"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codecommit"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codedeploy"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codepipeline"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cognitoidp"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/comprehend"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/configservice"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dms"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/docdb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ec2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecr"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/eks"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticache"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticloadbalancing"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticsearch"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elastictranscoder"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/emr"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/firehose"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glue"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/guardduty"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/inspector"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iot"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kafka"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lakeformation"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lexmodels"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lightsail"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/logs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lookoutmetrics"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/monitoring"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mq"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/neptune"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/opensearch"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/organizations"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/pipes"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/qldb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/rds"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/redshift"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/route53"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3tables"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/scheduler"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/secretsmanager"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/securityhub"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/servicediscovery"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssoadmin"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/swf"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/tagging"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/timestream"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/transfer"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/workspaces"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/xray"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcp/gcs"
)
