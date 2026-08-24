package edge

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
)

func TestDemuxQueryServiceAliases(t *testing.T) {
	server := &Server{bundle: catalog.Bundle()}
	cases := []struct{ prefix, service string }{
		{"s3-control", "aws.s3control"}, {"s3tables", "aws.s3tables"}, {"s3", "aws.s3"},
		{"sts", "aws.sts"}, {"sns", "aws.sns"}, {"iam", "aws.iam"}, {"sqs", "aws.sqs"},
		{"cloudformation", "aws.cloudformation"}, {"monitoring", "aws.monitoring"}, {"rds", "aws.rds"},
		{"docdb", "aws.docdb"}, {"neptune", "aws.neptune"}, {"elasticloadbalancing", "aws.elasticloadbalancing"},
		{"elasticache", "aws.elasticache"}, {"autoscaling", "aws.autoscaling"}, {"redshift", "aws.redshift"},
		{"lambda", "aws.lambda"}, {"apigateway", "aws.apigateway"}, {"route53resolver", "aws.route53resolver"},
		{"route53", "aws.route53"}, {"ec2", "aws.ec2"}, {"ses", "aws.ses"}, {"cognito-idp", "aws.cognito-idp"},
		{"cloudfront", "aws.cloudfront"}, {"elasticsearch", "aws.elasticsearch"}, {"opensearch", "aws.es"},
		{"glue", "aws.glue"}, {"athena", "aws.athena"}, {"cloudtrail", "aws.cloudtrail"},
		{"organizations", "aws.organizations"}, {"config", "aws.config"}, {"xray", "aws.xray"},
		{"guardduty", "aws.guardduty"}, {"mq", "aws.mq"}, {"iotwireless", "aws.iotwireless"},
		{"iot-data", "aws.iot-data"}, {"iot", "aws.iot"}, {"pipes", "aws.pipes"},
		{"codepipeline", "aws.codepipeline"}, {"appsync", "aws.appsync"}, {"apigatewayv2", "aws.apigatewayv2"},
		{"codecommit", "aws.codecommit"}, {"codedeploy", "aws.codedeploy"}, {"amplify", "aws.amplify"},
		{"inspector", "aws.inspector"}, {"securityhub", "aws.securityhub"}, {"timestream", "aws.timestream"},
		{"qldb", "aws.qldb"}, {"dms", "aws.dms"}, {"mediaconvert", "aws.mediaconvert"},
		{"elasticbeanstalk", "aws.elasticbeanstalk"}, {"swf", "aws.swf"}, {"elasticfilesystem", "aws.elasticfilesystem"},
		{"glacier", "aws.glacier"}, {"servicediscovery", "aws.servicediscovery"}, {"ram", "aws.ram"},
		{"sagemaker", "aws.sagemaker"}, {"workspaces", "aws.workspaces"}, {"transcribe", "aws.transcribe"},
		{"rekognition", "aws.rekognition"}, {"comprehendmedical", "aws.comprehendmedical"}, {"comprehend", "aws.comprehend"},
		{"mediastore", "aws.mediastore"}, {"kinesisanalyticsv2", "aws.kinesisanalyticsv2"},
		{"kinesisanalytics", "aws.kinesisanalytics"}, {"translate", "aws.translate"}, {"textract", "aws.textract"},
		{"polly", "aws.polly"}, {"fsx", "aws.fsx"}, {"servicecatalog", "aws.servicecatalog"},
		{"shield", "aws.shield"}, {"wafv2", "aws.wafv2"}, {"waf", "aws.waf"},
		{"storagegateway", "aws.storagegateway"}, {"lakeformation", "aws.lakeformation"}, {"connect", "aws.connect"},
		{"pinpoint", "aws.pinpoint"}, {"dax", "aws.dax"}, {"memorydb", "aws.memorydb"},
		{"keyspaces", "aws.keyspaces"}, {"mwaa", "aws.mwaa"}, {"sso-admin", "aws.sso-admin"},
		{"acm-pca", "aws.acm-pca"}, {"lightsail", "aws.lightsail"}, {"location", "aws.location"},
		{"kendra", "aws.kendra"}, {"quicksight", "aws.quicksight"}, {"identitystore", "aws.identitystore"},
		{"workmail", "aws.workmail"}, {"directconnect", "aws.directconnect"}, {"directoryservice", "aws.ds"},
		{"gamelift", "aws.gamelift"}, {"forecast", "aws.forecast"}, {"personalize", "aws.personalize"},
		{"lex-models", "aws.lex-models"}, {"medialive", "aws.medialive"}, {"mediapackage", "aws.mediapackage"},
		{"mediaconnect", "aws.mediaconnect"}, {"elastictranscoder", "aws.elastictranscoder"},
		{"cloudhsmv2", "aws.cloudhsmv2"}, {"macie2", "aws.macie2"}, {"access-analyzer", "aws.access-analyzer"},
		{"frauddetector", "aws.frauddetector"}, {"appmesh", "aws.appmesh"}, {"healthlake", "aws.healthlake"},
		{"lookoutmetrics", "aws.lookoutmetrics"}, {"bedrock", "aws.bedrock"}, {"fis", "aws.fis"}, {"ce", "aws.ce"},
		{"resource-groups", "aws.resource-groups"}, {"verifiedpermissions", "aws.verifiedpermissions"},
		{"support", "aws.support"}, {"codeartifact", "aws.codeartifact"}, {"cloudcontrol", "aws.cloudcontrol"},
		{"serverlessrepo", "aws.serverlessrepo"}, {"account", "aws.account"}, {"synthetics", "aws.synthetics"},
		{"apprunner", "aws.apprunner"}, {"proton", "aws.proton"}, {"resiliencehub", "aws.resiliencehub"},
		{"resource-explorer-2", "aws.resource-explorer-2"}, {"rum", "aws.rum"}, {"schemas", "aws.schemas"},
		{"dsql", "aws.dsql"}, {"codeconnections", "aws.codeconnections"}, {"managedblockchain", "aws.managedblockchain"},
	}
	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://localhost/?Action=Test", nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/"+tc.prefix+"/aws4_request")
			service := server.demux(req)
			if service == nil || service.ID != tc.service {
				t.Fatalf("demux %q = %#v, want %s", tc.prefix, service, tc.service)
			}
		})
	}
}

func TestDemuxTargetsAndPaths(t *testing.T) {
	server := &Server{bundle: catalog.Bundle()}
	cases := []struct {
		method, target, host, path, service string
	}{
		{http.MethodPost, "DynamoDBStreams_20120810.ListStreams", "localhost", "/", "aws.dynamodb"},
		{http.MethodPost, "DynamoDB_20120810.ListTables", "localhost", "/", "aws.dynamodb"},
		{http.MethodGet, "", "bucket.s3.us-east-1.amazonaws.com", "/key", "aws.s3"},
		{http.MethodGet, "", "localhost", "/storage/v1/b", "gcp.storage"},
		{http.MethodPost, "", "localhost", "/2015-03-31/functions", "aws.lambda"},
		{http.MethodGet, "", "localhost", "/restapis", "aws.apigateway"},
		{http.MethodGet, "", "localhost", "/2013-04-01/hostedzone", "aws.route53"},
		{http.MethodGet, "", "localhost", "/2020-05-31/distribution", "aws.cloudfront"},
		{http.MethodGet, "", "localhost", "/_search", "aws.es"},
		{http.MethodPut, "", "localhost", "/bucket/key", "aws.s3"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, "http://"+tc.host+tc.path, nil)
		req.Header.Set("X-Amz-Target", tc.target)
		service := server.demux(req)
		if service == nil || service.ID != tc.service {
			t.Fatalf("demux %s %s = %#v, want %s", tc.method, tc.path, service, tc.service)
		}
	}
	unknown := httptest.NewRequest(http.MethodPost, "http://localhost/", nil)
	unknown.Header.Set("X-Amz-Target", "Unknown.Target")
	if service := server.demux(unknown); service != nil {
		t.Fatalf("unknown target demuxed to %s", service.ID)
	}
}
