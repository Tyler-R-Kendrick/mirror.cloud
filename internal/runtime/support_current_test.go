package runtime

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/accessanalyzer"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/account"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/acm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/acmpca"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/amplify"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigateway"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigatewayv2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/appconfig"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/applicationautoscaling"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/appmesh"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apprunner"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/appsync"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/athena"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/autoscaling"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/awssupport"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/backup"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/batch"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/bedrock"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ce"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudcontrol"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudformation"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudfront"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudhsmv2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudtrail"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codeartifact"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codebuild"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codecommit"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codeconnections"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codedeploy"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/codepipeline"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cognitoidentity"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cognitoidp"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/comprehend"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/comprehendmedical"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/configservice"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/connect"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dax"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/directconnect"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/directoryservice"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dms"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/docdb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dsql"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ec2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecr"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ecs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/efs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/eks"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticache"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticbeanstalk"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticloadbalancing"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticsearch"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elastictranscoder"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/emr"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/firehose"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/fis"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/forecast"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/frauddetector"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/fsx"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/gamelift"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glacier"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/glue"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/guardduty"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/healthlake"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/identitystore"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/inspector"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iot"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iotdata"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iotwireless"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kafka"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kendra"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/keyspaces"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesisanalytics"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesisanalyticsv2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lakeformation"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lexmodels"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lightsail"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/location"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/logs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lookoutmetrics"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/macie2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/managedblockchain"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mediaconnect"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mediaconvert"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/medialive"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mediapackage"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mediastore"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/memorydb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/monitoring"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mq"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/mwaa"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/neptune"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/opensearch"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/organizations"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/personalize"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/pinpoint"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/pipes"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/polly"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/proton"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/qldb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/quicksight"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ram"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/rds"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/redshift"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/rekognition"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/resiliencehub"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/resourceexplorer2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/resourcegroups"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/route53"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/route53resolver"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/rum"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3control"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3tables"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sagemaker"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/scheduler"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/schemas"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/secretsmanager"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/securityhub"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/serverlessrepo"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/servicecatalog"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/servicediscovery"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ses"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sesv2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/shield"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssm"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssoadmin"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/storagegateway"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/swf"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/synthetics"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/tagging"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/textract"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/timestream"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/transcribe"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/transfer"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/translate"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/verifiedpermissions"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/waf"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/wafv2"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/workmail"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/workspaces"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/xray"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/gcp/gcs"
)

func TestSupportMatrixMatchesDocs(t *testing.T) {
	got := SupportMatrix()
	root := findGoMod(t)
	want, err := os.ReadFile(filepath.Join(root, "docs", "SUPPORT.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("docs/SUPPORT.md stale; run `mirror support-matrix`\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

func TestSupportMatrixEmulateCountsMatchPacks(t *testing.T) {
	want := packEmulateCounts(t)
	if len(want) == 0 {
		t.Fatal("no emulate packs registered")
	}
	for _, r := range SupportRows() {
		n := want[r.ID]
		if r.Emulate != n {
			t.Errorf("%s: SupportRows emulate=%d pack Operations=%d", r.ID, r.Emulate, n)
		}
		if r.Emulate > n {
			t.Errorf("%s: emulate count %d exceeds pack Operations() %d", r.ID, r.Emulate, n)
		}
	}
	root := findGoMod(t)
	docs, err := os.ReadFile(filepath.Join(root, "docs", "SUPPORT.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(docs), "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		cols := strings.Split(line, "|")
		if len(cols) < 5 {
			continue
		}
		id := strings.Trim(strings.TrimSpace(cols[1]), "`")
		emu, err := strconv.Atoi(strings.TrimSpace(cols[3]))
		if err != nil {
			continue
		}
		n := want[id]
		if emu > n {
			t.Errorf("docs/SUPPORT.md %s emulate %d > pack Operations() %d", id, emu, n)
		}
	}
}

func packEmulateCounts(t *testing.T) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, f := range registry.Factories() {
		if f.Tier != model.TierEmulate {
			continue
		}
		p, err := f.New(spi.Deps{})
		if err != nil || p == nil {
			t.Fatalf("%s: %v", f.ServiceID, err)
		}
		out[f.ServiceID] = len(p.Operations())
	}
	return out
}

func findGoMod(t *testing.T) string {
	t.Helper()
	wd, _ := os.Getwd()
	for p := wd; p != "/"; p = filepath.Dir(p) {
		if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
			return p
		}
	}
	t.Fatal("go.mod")
	return ""
}
