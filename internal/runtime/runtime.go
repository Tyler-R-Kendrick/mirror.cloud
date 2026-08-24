// Package runtime assembles SPI dependencies and the HTTP edge.
package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/blobs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bus"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/journal"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/store"
)

// Version is the binary version, overridable by the linker.
var Version = "0.1.0-dev"

// Runtime is a booted emulator process.
type Runtime struct {
	Cfg  config.Config
	Deps spi.Deps
	Reg  registry.Registry
	HTTP *edge.Server
}

// Aliases maps CLI names onto canonical service IDs.
var Aliases = map[string]string{
	"s3":                          "aws.s3",
	"aws.s3":                      "aws.s3",
	"dynamodb":                    "aws.dynamodb",
	"ddb":                         "aws.dynamodb",
	"aws.dynamodb":                "aws.dynamodb",
	"sqs":                         "aws.sqs",
	"aws.sqs":                     "aws.sqs",
	"sns":                         "aws.sns",
	"aws.sns":                     "aws.sns",
	"sts":                         "aws.sts",
	"aws.sts":                     "aws.sts",
	"iam":                         "aws.iam",
	"aws.iam":                     "aws.iam",
	"ssm":                         "aws.ssm",
	"aws.ssm":                     "aws.ssm",
	"secretsmanager":              "aws.secretsmanager",
	"secrets":                     "aws.secretsmanager",
	"sm":                          "aws.secretsmanager",
	"kms":                         "aws.kms",
	"aws.kms":                     "aws.kms",
	"logs":                        "aws.logs",
	"aws.logs":                    "aws.logs",
	"events":                      "aws.events",
	"eventbridge":                 "aws.events",
	"aws.events":                  "aws.events",
	"lambda":                      "aws.lambda",
	"aws.lambda":                  "aws.lambda",
	"cloudformation":              "aws.cloudformation",
	"cfn":                         "aws.cloudformation",
	"aws.cloudformation":          "aws.cloudformation",
	"kinesis":                     "aws.kinesis",
	"aws.kinesis":                 "aws.kinesis",
	"apigateway":                  "aws.apigateway",
	"apigw":                       "aws.apigateway",
	"aws.apigateway":              "aws.apigateway",
	"monitoring":                  "aws.monitoring",
	"cloudwatch":                  "aws.monitoring",
	"aws.monitoring":              "aws.monitoring",
	"route53":                     "aws.route53",
	"aws.route53":                 "aws.route53",
	"acm":                         "aws.acm",
	"aws.acm":                     "aws.acm",
	"rds":                         "aws.rds",
	"aws.rds":                     "aws.rds",
	"ecs":                         "aws.ecs",
	"aws.ecs":                     "aws.ecs",
	"elbv2":                       "aws.elasticloadbalancing",
	"elb":                         "aws.elasticloadbalancing",
	"elasticloadbalancing":        "aws.elasticloadbalancing",
	"aws.elasticloadbalancing":    "aws.elasticloadbalancing",
	"elasticache":                 "aws.elasticache",
	"aws.elasticache":             "aws.elasticache",
	"autoscaling":                 "aws.autoscaling",
	"asg":                         "aws.autoscaling",
	"aws.autoscaling":             "aws.autoscaling",
	"ecr":                         "aws.api.ecr",
	"aws.api.ecr":                 "aws.api.ecr",
	"tagging":                     "aws.tagging",
	"aws.tagging":                 "aws.tagging",
	"application-autoscaling":     "aws.application-autoscaling",
	"aws.application-autoscaling": "aws.application-autoscaling",
	"redshift":                    "aws.redshift",
	"aws.redshift":                "aws.redshift",
	"eks":                         "aws.eks",
	"aws.eks":                     "aws.eks",
	"ec2":                         "aws.ec2",
	"aws.ec2":                     "aws.ec2",
	"ses":                         "aws.ses",
	"aws.ses":                     "aws.ses",
	"cognito-idp":                 "aws.cognito-idp",
	"cognito":                     "aws.cognito-idp",
	"aws.cognito-idp":             "aws.cognito-idp",
	"states":                      "aws.states",
	"sfn":                         "aws.states",
	"stepfunctions":               "aws.states",
	"aws.states":                  "aws.states",
	"firehose":                    "aws.firehose",
	"aws.firehose":                "aws.firehose",
	"cloudfront":                  "aws.cloudfront",
	"aws.cloudfront":              "aws.cloudfront",
	"scheduler":                   "aws.scheduler",
	"aws.scheduler":               "aws.scheduler",
	"es":                          "aws.es",
	"opensearch":                  "aws.es",
	"aws.es":                      "aws.es",
	"aws.opensearch":              "aws.es",
	"glue":                        "aws.glue",
	"aws.glue":                    "aws.glue",
	"athena":                      "aws.athena",
	"aws.athena":                  "aws.athena",
	"cloudtrail":                  "aws.cloudtrail",
	"aws.cloudtrail":              "aws.cloudtrail",
	"organizations":               "aws.organizations",
	"aws.organizations":           "aws.organizations",
	"transfer":                    "aws.transfer",
	"aws.transfer":                "aws.transfer",
	"wafv2":                       "aws.wafv2",
	"aws.wafv2":                   "aws.wafv2",
	"appconfig":                   "aws.appconfig",
	"aws.appconfig":               "aws.appconfig",
	"codebuild":                   "aws.codebuild",
	"aws.codebuild":               "aws.codebuild",
	"batch":                       "aws.batch",
	"aws.batch":                   "aws.batch",
	"emr":                         "aws.elasticmapreduce",
	"elasticmapreduce":            "aws.elasticmapreduce",
	"aws.elasticmapreduce":        "aws.elasticmapreduce",
	"kafka":                       "aws.kafka",
	"msk":                         "aws.kafka",
	"aws.kafka":                   "aws.kafka",
	"backup":                      "aws.backup",
	"aws.backup":                  "aws.backup",
	"cognito-identity":            "aws.cognito-identity",
	"aws.cognito-identity":        "aws.cognito-identity",
	"sesv2":                       "aws.sesv2",
	"aws.sesv2":                   "aws.sesv2",
	"config":                      "aws.config",
	"aws.config":                  "aws.config",
	"xray":                        "aws.xray",
	"aws.xray":                    "aws.xray",
	"guardduty":                   "aws.guardduty",
	"aws.guardduty":               "aws.guardduty",
	"mq":                          "aws.mq",
	"aws.mq":                      "aws.mq",
	"docdb":                       "aws.docdb",
	"aws.docdb":                   "aws.docdb",
	"iot":                         "aws.iot",
	"aws.iot":                     "aws.iot",
	"pipes":                       "aws.pipes",
	"aws.pipes":                   "aws.pipes",
	"codepipeline":                "aws.codepipeline",
	"aws.codepipeline":            "aws.codepipeline",
	"appsync":                     "aws.appsync",
	"aws.appsync":                 "aws.appsync",
	"apigatewayv2":                "aws.apigatewayv2",
	"aws.apigatewayv2":            "aws.apigatewayv2",
	"codecommit":                  "aws.codecommit",
	"aws.codecommit":              "aws.codecommit",
	"codedeploy":                  "aws.codedeploy",
	"aws.codedeploy":              "aws.codedeploy",
	"amplify":                     "aws.amplify",
	"aws.amplify":                 "aws.amplify",
	"inspector":                   "aws.inspector",
	"aws.inspector":               "aws.inspector",
	"securityhub":                 "aws.securityhub",
	"aws.securityhub":             "aws.securityhub",
	"neptune":                     "aws.neptune",
	"aws.neptune":                 "aws.neptune",
	"timestream":                  "aws.timestream",
	"timestream-write":            "aws.timestream",
	"timestream-query":            "aws.timestream",
	"aws.timestream":              "aws.timestream",
	"qldb":                        "aws.qldb",
	"aws.qldb":                    "aws.qldb",
	"dms":                         "aws.dms",
	"aws.dms":                     "aws.dms",
	"mediaconvert":                "aws.mediaconvert",
	"aws.mediaconvert":            "aws.mediaconvert",
	"elasticsearch":               "aws.elasticsearch",
	"aws.elasticsearch":           "aws.elasticsearch",
	"elasticbeanstalk":            "aws.elasticbeanstalk",
	"aws.elasticbeanstalk":        "aws.elasticbeanstalk",
	"swf":                         "aws.swf",
	"aws.swf":                     "aws.swf",
	"efs":                         "aws.elasticfilesystem",
	"elasticfilesystem":           "aws.elasticfilesystem",
	"aws.elasticfilesystem":       "aws.elasticfilesystem",
	"aws.efs":                     "aws.elasticfilesystem",
	"glacier":                     "aws.glacier",
	"aws.glacier":                 "aws.glacier",
	"servicediscovery":            "aws.servicediscovery",
	"cloudmap":                    "aws.servicediscovery",
	"aws.servicediscovery":        "aws.servicediscovery",
	"ram":                         "aws.ram",
	"aws.ram":                     "aws.ram",
	"sagemaker":                   "aws.sagemaker",
	"aws.sagemaker":               "aws.sagemaker",
	"workspaces":                  "aws.workspaces",
	"aws.workspaces":              "aws.workspaces",
	"transcribe":                  "aws.transcribe",
	"aws.transcribe":              "aws.transcribe",
	"rekognition":                 "aws.rekognition",
	"aws.rekognition":             "aws.rekognition",
	"comprehend":                  "aws.comprehend",
	"aws.comprehend":              "aws.comprehend",
	"mediastore":                  "aws.mediastore",
	"aws.mediastore":              "aws.mediastore",
	"kinesisanalytics":            "aws.kinesisanalytics",
	"aws.kinesisanalytics":        "aws.kinesisanalytics",
	"translate":                   "aws.translate",
	"aws.translate":               "aws.translate",
	"textract":                    "aws.textract",
	"aws.textract":                "aws.textract",
	"polly":                       "aws.polly",
	"aws.polly":                   "aws.polly",
	"fsx":                         "aws.fsx",
	"aws.fsx":                     "aws.fsx",
	"s3control":                   "aws.s3control",
	"s3-control":                  "aws.s3control",
	"aws.s3control":               "aws.s3control",
	"route53resolver":             "aws.route53resolver",
	"aws.route53resolver":         "aws.route53resolver",
	"servicecatalog":              "aws.servicecatalog",
	"aws.servicecatalog":          "aws.servicecatalog",
	"shield":                      "aws.shield",
	"aws.shield":                  "aws.shield",
	"waf":                         "aws.waf",
	"aws.waf":                     "aws.waf",
	"storagegateway":              "aws.storagegateway",
	"aws.storagegateway":          "aws.storagegateway",
	"lakeformation":               "aws.lakeformation",
	"aws.lakeformation":           "aws.lakeformation",
	"connect":                     "aws.connect",
	"aws.connect":                 "aws.connect",
	"pinpoint":                    "aws.pinpoint",
	"mobiletargeting":             "aws.pinpoint",
	"aws.pinpoint":                "aws.pinpoint",
	"dax":                         "aws.dax",
	"aws.dax":                     "aws.dax",
	"memorydb":                    "aws.memorydb",
	"aws.memorydb":                "aws.memorydb",
	"keyspaces":                   "aws.keyspaces",
	"cassandra":                   "aws.keyspaces",
	"aws.keyspaces":               "aws.keyspaces",
	"mwaa":                        "aws.mwaa",
	"airflow":                     "aws.mwaa",
	"aws.mwaa":                    "aws.mwaa",
	"sso":                         "aws.sso-admin",
	"sso-admin":                   "aws.sso-admin",
	"ssoadmin":                    "aws.sso-admin",
	"aws.sso-admin":               "aws.sso-admin",
	"acm-pca":                     "aws.acm-pca",
	"acmpca":                      "aws.acm-pca",
	"aws.acm-pca":                 "aws.acm-pca",
	"lightsail":                   "aws.lightsail",
	"aws.lightsail":               "aws.lightsail",
	"location":                    "aws.location",
	"geo":                         "aws.location",
	"aws.location":                "aws.location",
	"kendra":                      "aws.kendra",
	"aws.kendra":                  "aws.kendra",
	"quicksight":                  "aws.quicksight",
	"aws.quicksight":              "aws.quicksight",
	"identitystore":               "aws.identitystore",
	"aws.identitystore":           "aws.identitystore",
	"workmail":                    "aws.workmail",
	"aws.workmail":                "aws.workmail",
	"directconnect":               "aws.directconnect",
	"aws.directconnect":           "aws.directconnect",
	"ds":                          "aws.ds",
	"directoryservice":            "aws.ds",
	"aws.ds":                      "aws.ds",
	"gamelift":                    "aws.gamelift",
	"aws.gamelift":                "aws.gamelift",
	"forecast":                    "aws.forecast",
	"aws.forecast":                "aws.forecast",
	"personalize":                 "aws.personalize",
	"aws.personalize":             "aws.personalize",
	"lex":                         "aws.lex-models",
	"lex-models":                  "aws.lex-models",
	"aws.lex-models":              "aws.lex-models",
	"medialive":                   "aws.medialive",
	"aws.medialive":               "aws.medialive",
	"mediapackage":                "aws.mediapackage",
	"aws.mediapackage":            "aws.mediapackage",
	"mediaconnect":                "aws.mediaconnect",
	"aws.mediaconnect":            "aws.mediaconnect",
	"elastictranscoder":           "aws.elastictranscoder",
	"aws.elastictranscoder":       "aws.elastictranscoder",
	"cloudhsm":                    "aws.cloudhsmv2",
	"cloudhsmv2":                  "aws.cloudhsmv2",
	"aws.cloudhsmv2":              "aws.cloudhsmv2",
	"macie":                       "aws.macie2",
	"macie2":                      "aws.macie2",
	"aws.macie2":                  "aws.macie2",
	"accessanalyzer":              "aws.access-analyzer",
	"access-analyzer":             "aws.access-analyzer",
	"aws.access-analyzer":         "aws.access-analyzer",
	"comprehendmedical":           "aws.comprehendmedical",
	"aws.comprehendmedical":       "aws.comprehendmedical",
	"frauddetector":               "aws.frauddetector",
	"aws.frauddetector":           "aws.frauddetector",
	"appmesh":                     "aws.appmesh",
	"aws.appmesh":                 "aws.appmesh",
	"healthlake":                  "aws.healthlake",
	"aws.healthlake":              "aws.healthlake",
	"lookoutmetrics":              "aws.lookoutmetrics",
	"aws.lookoutmetrics":          "aws.lookoutmetrics",
	"bedrock":                     "aws.bedrock",
	"aws.bedrock":                 "aws.bedrock",
	"fis":                         "aws.fis",
	"aws.fis":                     "aws.fis",
	"ce":                          "aws.ce",
	"aws.ce":                      "aws.ce",
	"resource-groups":             "aws.resource-groups",
	"resourcegroups":              "aws.resource-groups",
	"aws.resource-groups":         "aws.resource-groups",
	"verifiedpermissions":         "aws.verifiedpermissions",
	"aws.verifiedpermissions":     "aws.verifiedpermissions",
	"support":                     "aws.support",
	"aws.support":                 "aws.support",
	"codeartifact":                "aws.codeartifact",
	"aws.codeartifact":            "aws.codeartifact",
	"cloudcontrol":                "aws.cloudcontrol",
	"cloudcontrolapi":             "aws.cloudcontrol",
	"aws.cloudcontrol":            "aws.cloudcontrol",
	"serverlessrepo":              "aws.serverlessrepo",
	"aws.serverlessrepo":          "aws.serverlessrepo",
	"account":                     "aws.account",
	"aws.account":                 "aws.account",
	"iotwireless":                 "aws.iotwireless",
	"api.iotwireless":             "aws.iotwireless",
	"aws.iotwireless":             "aws.iotwireless",
	"s3tables":                    "aws.s3tables",
	"aws.s3tables":                "aws.s3tables",
	"synthetics":                  "aws.synthetics",
	"aws.synthetics":              "aws.synthetics",
	"apprunner":                   "aws.apprunner",
	"aws.apprunner":               "aws.apprunner",
	"proton":                      "aws.proton",
	"aws.proton":                  "aws.proton",
	"resiliencehub":               "aws.resiliencehub",
	"aws.resiliencehub":           "aws.resiliencehub",
	"resource-explorer-2":         "aws.resource-explorer-2",
	"resourceexplorer2":           "aws.resource-explorer-2",
	"resource-explorer":           "aws.resource-explorer-2",
	"aws.resource-explorer-2":     "aws.resource-explorer-2",
	"rum":                         "aws.rum",
	"aws.rum":                     "aws.rum",
	"schemas":                     "aws.schemas",
	"aws.schemas":                 "aws.schemas",
	"dsql":                        "aws.dsql",
	"aws.dsql":                    "aws.dsql",
	"codeconnections":             "aws.codeconnections",
	"aws.codeconnections":         "aws.codeconnections",
	"iot-data":                    "aws.iot-data",
	"iotdata":                     "aws.iot-data",
	"data.iot":                    "aws.iot-data",
	"aws.iot-data":                "aws.iot-data",
	"managedblockchain":           "aws.managedblockchain",
	"aws.managedblockchain":       "aws.managedblockchain",
	"kinesisanalyticsv2":          "aws.kinesisanalyticsv2",
	"aws.kinesisanalyticsv2":      "aws.kinesisanalyticsv2",
	"gcs":                         "gcp.storage",
	"storage":                     "gcp.storage",
	"gcp.storage":                 "gcp.storage",
}

// Profiles are named service sets.
var Profiles = map[string][]string{
	"aws-core": {"aws.s3", "aws.dynamodb", "aws.sqs", "aws.sns", "aws.sts", "aws.iam", "aws.ssm", "aws.secretsmanager", "aws.kms", "aws.logs", "aws.events", "aws.lambda", "aws.cloudformation", "aws.kinesis", "aws.apigateway", "aws.monitoring", "aws.route53", "aws.acm", "aws.rds", "aws.ecs", "aws.elasticloadbalancing", "aws.elasticache", "aws.autoscaling", "aws.api.ecr", "aws.tagging", "aws.application-autoscaling", "aws.redshift", "aws.eks", "aws.ec2", "aws.ses", "aws.cognito-idp", "aws.states", "aws.firehose", "aws.cloudfront", "aws.scheduler", "aws.es", "aws.glue", "aws.athena", "aws.cloudtrail", "aws.organizations", "aws.transfer", "aws.wafv2", "aws.appconfig", "aws.codebuild", "aws.batch", "aws.elasticmapreduce", "aws.kafka", "aws.backup", "aws.cognito-identity", "aws.sesv2", "aws.config", "aws.xray", "aws.guardduty", "aws.mq", "aws.docdb", "aws.iot", "aws.pipes", "aws.codepipeline", "aws.appsync", "aws.apigatewayv2", "aws.codecommit", "aws.codedeploy", "aws.amplify", "aws.inspector", "aws.securityhub", "aws.neptune", "aws.timestream", "aws.qldb", "aws.dms", "aws.mediaconvert", "aws.elasticsearch", "aws.elasticbeanstalk", "aws.swf", "aws.elasticfilesystem", "aws.glacier", "aws.servicediscovery", "aws.ram", "aws.sagemaker", "aws.workspaces", "aws.transcribe", "aws.rekognition", "aws.comprehend", "aws.mediastore", "aws.kinesisanalytics", "aws.translate", "aws.textract", "aws.polly", "aws.fsx", "aws.s3control", "aws.route53resolver", "aws.servicecatalog", "aws.shield", "aws.waf", "aws.storagegateway", "aws.lakeformation", "aws.connect", "aws.pinpoint", "aws.dax", "aws.memorydb", "aws.keyspaces", "aws.mwaa", "aws.sso-admin", "aws.acm-pca", "aws.lightsail", "aws.location", "aws.kendra", "aws.quicksight", "aws.identitystore", "aws.workmail", "aws.directconnect", "aws.ds", "aws.gamelift", "aws.forecast", "aws.personalize", "aws.lex-models", "aws.medialive", "aws.mediapackage", "aws.mediaconnect", "aws.elastictranscoder", "aws.cloudhsmv2", "aws.macie2", "aws.access-analyzer", "aws.comprehendmedical", "aws.frauddetector", "aws.appmesh", "aws.healthlake", "aws.lookoutmetrics", "aws.bedrock", "aws.fis", "aws.ce", "aws.resource-groups", "aws.verifiedpermissions", "aws.support", "aws.codeartifact", "aws.cloudcontrol", "aws.serverlessrepo", "aws.account", "aws.iotwireless", "aws.s3tables", "aws.synthetics", "aws.apprunner", "aws.proton", "aws.resiliencehub", "aws.resource-explorer-2", "aws.rum", "aws.schemas", "aws.dsql", "aws.codeconnections", "aws.iot-data", "aws.managedblockchain", "aws.kinesisanalyticsv2"},
	"gcp-core": {"gcp.storage"},
}

// CanonicalServiceID maps a CLI token to a service ID.
func CanonicalServiceID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if id, ok := Aliases[s]; ok {
		return id
	}
	return s
}

// ExpandServices resolves CLI tokens, comma lists, and profiles.
func ExpandServices(args []string, profile string, all bool) []string {
	if all {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	add := func(id string) {
		id = CanonicalServiceID(id)
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	if profile != "" {
		if ids, ok := Profiles[profile]; ok {
			for _, id := range ids {
				add(id)
			}
		}
	}
	for _, a := range args {
		for _, p := range strings.Split(a, ",") {
			add(p)
		}
	}
	return out
}

// Boot constructs dependencies, registry, and HTTP server.
func Boot(cfg config.Config) (*Runtime, error) {
	clk := spi.Clock(clock.Real{})
	if strings.EqualFold(os.Getenv("MIRROR_CLOCK"), "controllable") {
		c := clock.NewControllable()
		_ = c.Advance(1577836800 * 1e9) // 2020-01-01 UTC
		clk = c
	}
	base := specboot.Bundle()
	bundle := *base
	bundle.Services = append([]model.Service(nil), base.Services...)
	for i := range bundle.Services {
		bundle.Services[i].Operations = append([]model.Operation(nil), base.Services[i].Operations...)
	}
	deps := spi.Deps{
		Store:   store.NewMemory(cfg.LockSHA),
		Blobs:   blobs.NewMemory(),
		Bus:     bus.New(),
		Clock:   clk,
		Rand:    rand.New(cfg.Seed),
		Journal: journal.New(),
		Model:   &bundle,
	}
	deps.Authorizer = iam.NewAuthorizer(deps.Store)
	if cfg.PersistDir != "" {
		if err := os.MkdirAll(cfg.PersistDir, 0o755); err != nil {
			return nil, err
		}
		if b, err := os.ReadFile(filepath.Join(cfg.PersistDir, "state.tar")); err == nil {
			if err := Restore(deps, cfg.LockSHA, bytes.NewReader(b)); err != nil {
				return nil, fmt.Errorf("restore persist: %w", err)
			}
		}
	}
	reg, err := registry.New(deps, cfg.Services, cfg.Tiers)
	if err != nil {
		return nil, err
	}
	for _, id := range reg.Enabled() {
		pack, ok := reg.Resolve(id)
		svc := bundle.ServiceByID(id)
		if !ok || svc == nil {
			continue
		}
		for _, name := range pack.Operations() {
			if svc.OperationByName(name) == nil {
				svc.Operations = append(svc.Operations, model.Operation{Name: name, HTTP: model.HTTPBinding{Method: http.MethodPost, Code: http.StatusOK}})
			}
		}
	}
	return &Runtime{Cfg: cfg, Deps: deps, Reg: reg, HTTP: edge.New(cfg, deps, reg, Version)}, nil
}

// Handler is the HTTP handler.
func (rt *Runtime) Handler() http.Handler { return rt.HTTP }

// Close stops behavior packs before closing shared state.
func (rt *Runtime) Close() error {
	return errors.Join(rt.Reg.Close(), rt.Deps.Store.Close())
}

// SavePersist writes process state to cfg.PersistDir when set.
func (rt *Runtime) SavePersist() error {
	if rt.Cfg.PersistDir == "" {
		return nil
	}
	var buf bytes.Buffer
	if err := Snapshot(rt.Deps, rt.Cfg.LockSHA, &buf); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(rt.Cfg.PersistDir, "state.tar"), buf.Bytes(), 0o644)
}

// Snapshot writes store + blobs + lock into one tar archive.
func Snapshot(deps spi.Deps, lockSHA string, w io.Writer) error {
	var storeBuf, blobBuf bytes.Buffer
	if err := deps.Store.Snapshot(context.Background(), &storeBuf); err != nil {
		return err
	}
	if err := deps.Blobs.Snapshot(context.Background(), &blobBuf); err != nil {
		return err
	}
	man, _ := json.Marshal(map[string]string{
		"lockSHA": lockSHA,
		"seed":    "",
	})
	tw := tar.NewWriter(w)
	defer tw.Close()
	for _, f := range []struct {
		name string
		body []byte
	}{
		{"manifest.json", man},
		{"store.tar", storeBuf.Bytes()},
		{"blobs.json", blobBuf.Bytes()},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body))}); err != nil {
			return err
		}
		if _, err := tw.Write(f.body); err != nil {
			return err
		}
	}
	return nil
}

// Restore loads a Snapshot archive.
func Restore(deps spi.Deps, lockSHA string, r io.Reader) error {
	tr := tar.NewReader(r)
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		files[hdr.Name] = b
	}
	var man struct {
		LockSHA string `json:"lockSHA"`
	}
	if b, ok := files["manifest.json"]; ok {
		_ = json.Unmarshal(b, &man)
	}
	want := lockSHA
	if want != "" && man.LockSHA != "" && man.LockSHA != want {
		return fmt.Errorf("snapshot spec-lock hash %s does not match current %s", man.LockSHA, want)
	}
	if b := files["store.tar"]; len(b) > 0 {
		if err := deps.Store.Restore(context.Background(), bytes.NewReader(b)); err != nil {
			return err
		}
	}
	if b := files["blobs.json"]; len(b) > 0 {
		if err := deps.Blobs.Restore(context.Background(), bytes.NewReader(b)); err != nil {
			return err
		}
	}
	return nil
}

// SupportRow is one service in the generated support matrix.
type SupportRow struct {
	ID       string
	Protocol model.Protocol
	Emulate  int
	Mock     int
}

// SupportRows derives emulate counts from pack Operations(), not the full ingested spec.
func SupportRows() []SupportRow {
	b := catalog.Bundle()
	packOps := map[string]int{}
	for _, f := range registry.Factories() {
		if f.Tier != model.TierEmulate {
			continue
		}
		p, err := f.New(spi.Deps{})
		if err != nil || p == nil {
			continue
		}
		packOps[f.ServiceID] = len(p.Operations())
	}
	var rows []SupportRow
	for _, svc := range b.Services {
		emu := packOps[svc.ID]
		modelN := len(svc.Operations)
		mock := modelN - emu
		if mock < 0 {
			mock = 0
		}
		if emu == 0 {
			mock = modelN
		}
		rows = append(rows, SupportRow{ID: svc.ID, Protocol: svc.Protocol, Emulate: emu, Mock: mock})
	}
	return rows
}

// SupportMatrix renders docs/SUPPORT.md from pack Operations() plus mock remainder.
func SupportMatrix() string {
	var sb strings.Builder
	sb.WriteString("# Support matrix\n\n")
	sb.WriteString("Generated by `mirror support-matrix`. Do not hand-edit.\n\n")
	sb.WriteString("Emulate op counts are `len(pack.Operations())`. Remaining ingested operations are mock-tier (or `MirrorNotImplemented` under `--strict`).\n\n")
	sb.WriteString("| Service | Protocol | Emulate ops | Mock ops |\n|---|---|---|---|\n")
	for _, r := range SupportRows() {
		fmt.Fprintf(&sb, "| `%s` | `%s` | %d | %d |\n", r.ID, r.Protocol, r.Emulate, r.Mock)
	}
	sb.WriteString("\nIAM evaluates user, group, role, and Organizations SCP policies: explicit Deny, then Allow. A principal with policies and no matching Allow is denied; no policies still allows. Access keys and `X-Mirror-Role` select principals. SimulatePrincipalPolicy/SimulateCustomPolicy use the same evaluator. Ops not in pack Operations() are mock-tier (or 501 under --strict), not leftover-KV sold as emulate.\n")
	sb.WriteString("SSM `SecureString` uses a reversible local encoding, not real encryption. Leftover SSM ops (activations, sessions, inventory, patch groups, compliance, …) are control-plane records only — no SSM agent.\n")
	sb.WriteString("Lambda CreateFunction/Invoke run a local python3 or node handler when that interpreter exists; other runtimes return `MirrorNotImplemented`.\n")
	sb.WriteString("CloudFormation accepts JSON/YAML TemplateBody and TemplateURL objects from local S3. YAML is a maps/lists/scalars + !Ref/!GetAtt/!Sub subset. Supported resource types: AWS::S3::Bucket (including versioning, CORS, encryption, lifecycle, replication, notifications, tags, and ownership/public-access settings), AWS::SQS::Queue, AWS::SNS::Topic, AWS::DynamoDB::Table, AWS::IAM::Role, AWS::SSM::Parameter, AWS::SecretsManager::Secret, AWS::KMS::Key, AWS::Logs::LogGroup, AWS::Events::Rule, AWS::Lambda::Function, AWS::Kinesis::Stream, AWS::Kinesis::ResourcePolicy, AWS::ApiGateway::RestApi. Cloud Control reads/lists S3 buckets, API Gateway V2 APIs, and RDS instances/clusters from service state. Unknown types fail CloudFormation stacks.\n")
	sb.WriteString("Kinesis is a single in-memory shard per stream. DynamoDB Streams is a single shard per table; PutItem/DeleteItem/UpdateItem/BatchWrite emit records when StreamSpecification.StreamEnabled. No shard splitting or 24h retention. S3 SelectObjectContent projects CSV/JSON columns (`s._N` or header names) with optional WHERE col = lit — not Parquet and not aggregations.\n")
	sb.WriteString("API Gateway is REST + Lambda AWS_PROXY; invoke at `/restapis/{id}/{stage}/_user_request_/{path}`.\n")
	sb.WriteString("CloudWatch metrics store PutMetricData datapoints in memory; GetMetricStatistics is a sum/min/max/avg of those points, not AWS aggregation windows.\n")
	sb.WriteString("Route 53 is hosted zones + resource record sets only. ACM issues local untrusted certificates with Status=ISSUED immediately.\n")
	sb.WriteString("RDS/ECS/ELBv2/ElastiCache/Auto Scaling/ECR store control-plane records listed in Operations() only (no real database, containers, Redis, or registry daemon). ECS service task IPs register with ELBv2 target groups on RUNNING and deregister on STOPPED/FAILED.\n")
	sb.WriteString("EC2 is control-plane records (VPC/subnet/security group/instance ids) on the ec2Query wire — no hypervisor. Extra IAM/RDS/Redshift/API Gateway/SSM/GCS ops are named control-plane records, not leftover-KV sold as emulate.\n")
	sb.WriteString("Firehose preserves disabled data-format-conversion configuration, but enabled JSON-to-Parquet/ORC conversion returns `MirrorNotImplemented` instead of delivering falsely labeled raw bytes.\n")
	sb.WriteString("Firehose validates and describes MSK source configuration, consumes persisted and future local topic messages from the configured start timestamp, and ignores other clusters and topics; it does not expose or poll a Kafka broker.\n")
	sb.WriteString("Firehose validates and describes database source endpoint, pattern, Secrets Manager, and VPC configuration, but does not connect to a database or ingest changes.\n")
	sb.WriteString("Firehose consumes future records from local Kinesis source streams, automatically deaggregates valid KPL protobuf envelopes before processing, and rejects PutRecord and PutRecordBatch for Kinesis-, MSK-, and database-source streams.\n")
	sb.WriteString("Firehose accepts both legacy Elasticsearch and current Amazonopensearchservice destination wire shapes for local OpenSearch domain delivery, including VPC configuration descriptions.\n")
	sb.WriteString("Firehose validates, describes, and updates OpenSearch Serverless destinations, indexes processed JSON into collection-namespaced local OpenSearch indexes, persistently buffers and retries across restarts, backs up AllDocuments, and emits AWS-shaped failed-document envelopes; it does not emulate the OpenSearch Serverless collection control plane.\n")
	sb.WriteString("Step Functions implements all 37 current API operations. State-machine create/update supports AWS-shaped idempotency, revisions, tags, and publish-on-write; qualified version and alias lists, execution lists, history, and map-run lists return filtered, paginated summaries. Choice states support the complete JSONPath logical, comparison, path-comparison, wildcard, and type-test rule family.\n")
	sb.WriteString("Step Functions Task resources invoke optimized SQS SendMessage, SNS Publish, DynamoDB GetItem/PutItem/UpdateItem/DeleteItem, and nested StartExecution integrations, plus generic AWS SDK operations exposed by registered local behavior packs. Service faults use integration-specific AWS error prefixes. Standard workflows support .waitForTaskToken callbacks for optimized SQS, SNS, Lambda, and nested Step Functions with token injection, heartbeat, retry/catch, and stop invalidation. They also support .sync for locally terminal Batch SubmitJob, CodeBuild StartBuild, Glue StartJobRun, and EMR createCluster/addStep jobs.\n")
	sb.WriteString("Step Functions Task Credentials accept static or JSONPath-selected IAM role ARNs and invoke local integrations in the selected role and account scope.\n")
	sb.WriteString("Nested Step Functions StartExecution .sync and .sync:2 wait for local Standard child executions and return AWS string or JSON output envelopes.\n")
	sb.WriteString("Step Functions ECS RunTask.sync returns a terminal local task and persists its STOPPED status.\n")
	sb.WriteString("Distributed Map iterations persist Standard child execution output, history, and Map Run linkage for list and describe APIs.\n")
	sb.WriteString("Distributed Map ItemBatcher supports direct or JSONPath-selected item-count and byte limits plus resolved BatchInput envelopes.\n")
	sb.WriteString("Distributed Map ResultWriter supports NONE, COMPACT, and FLATTEN transformations in JSON or JSONL and exports status files plus a manifest to local S3.\n")
	sb.WriteString("Step Functions walks Pass/Succeed/Fail/Task/Choice/Parallel/Map, durably resumes Standard Wait states across restarts, and blocks synchronous Express executions until Wait deadlines, and enforces configured plus Standard one-year and Express five-minute execution deadlines with durable TIMED_OUT records. Task Resource `lambda:function` or `states:::lambda:invoke` calls the local Lambda pack. Lambda, Activity, Parallel, and inline Map states support ordered Retry/Catch policies, AWS error wildcards, default and explicit retry counts, error output, and null/root/nested catcher ResultPath; retry timing honors IntervalSeconds, BackoffRate, MaxDelaySeconds, and JitterStrategy; Standard retries resume durably across restarts and Express retries block synchronously. Activity retries persist their counts, input, and fresh task tokens across worker callbacks, claimed Activities and callback tasks durably enforce TimeoutSeconds and HeartbeatSeconds, heartbeats reset their deadline, and successful Activity Tasks apply ResultPath before resuming. Map ItemSelector and legacy Parameters select state input plus per-item Index, Value, and STATE_DATA Source context. Pass, Task, and Parallel Parameters plus Map ItemSelector/Parameters support all 18 AWS JSONPath intrinsic functions. Walked states apply InputPath and OutputPath, while Task, Parallel, and Map results apply ResultSelector then ResultPath; state and Task inputs, raw results, and outputs enforce the 256 KiB quota with terminal States.DataLimitExceeded before side effects. ValidateStateMachineDefinition reports structural ASL diagnostics, including ASL version, comments, and state-name limits, Choice rule structure and operand types, Pass query-language field rules, state-specific field ownership, Map processor and input container shapes, required nested configuration, and legacy-field exclusivity, JSONPath data-flow field shapes and ownership, Assign object shape and query-language variable names across states, Choice rules, and catchers, Wait and Fail field exclusivity and value types, state transition exclusivity and terminal-state rules, plus Catch ownership, shape, result paths, error lists, targets, and wildcard ordering, and create/update reject invalid definitions; TestState executes supported single states through the same data-flow walker. Published versions retain immutable definitions, and aliases route executions to validated weighted version configurations. Standard executions can be described from their definition snapshot and redriven from the unsuccessful state with preserved history and client-token idempotency; StopExecution validates and preserves optional error and cause details. Top-level Express executions reject describe, history, stop, and state-machine-for-execution APIs, while Express Distributed Map children remain describable. Parallel branches execute concurrently with isolated inputs and ordered results. Inline and array-backed Distributed Map iterations execute concurrently, preserve result order, and honor MaxConcurrency with 40 and 10,000 iteration ceilings respectively; array-backed Distributed Map states run synchronously, honor failure tolerances, record Map Run counts, and support describe/list/update control-plane APIs; S3 ItemReader supports JSON, JSONL, CSV, and Parquet. JSONata workflows support Arguments, Output, Assign, Choice Condition, Map Items/ItemSelector, inherited and JSONPath-mode variables, expression-aware Catch/Retry, Wait/Fail expressions, ItemReader/ItemBatcher/ResultWriter arguments, dynamic Credentials and Task/Map numeric fields, execution/state context, callback continuation, and the Step Functions partition/range/hash/random/uuid/parse functions. Firehose PutRecord applies validated JSON/JSONL or base64-delimited deaggregation with the AWS 500-subrecord fallback, GZIP CloudWatch Logs decompression, metadata-free DATA_MESSAGE extraction with CONTROL_MESSAGE dropping, raw-record decompression/processing-failure envelopes, AppendDelimiterToRecord processing, JQ-1.6 inline metadata extraction, and synchronous Lambda transformation that validates role and buffering hints but invokes per record, with bounded retries, drop handling, Lambda partition-key metadata, S3 processing-failure envelopes, and optional CloudWatch error logs before writing decoded, GZIP-compressed, ZIP-archived, Snappy-compressed, or Hadoop-Snappy-framed bytes to the configured S3 destination using default UTC or an AWS-syntax-validated named time zone, timestamp, random-string, Lambda-derived, and query-derived dynamic-partition prefix expressions with AWS success/error-prefix syntax and length validation, and AWS-style stream/version/timestamp suffixes with default or validated custom file extensions; dynamic partitioning validates create-only enablement, one-way lifecycle, retry settings and defaults, and per-subrecord missing-key failure delivery; PutRecord and PutRecordBatch validate base64 data, per-record size, batch count, and total batch size before storing anything; stream names are validated across operations, duplicate creates and missing deletes return AWS resource faults, and descriptions expose deterministic create/update timestamps; stream listing paginates alphabetically and filters by source type; tags validate, merge, selectively remove, and paginate; SSE state validates AWS-owned or customer-managed key configuration, persists across Describe, and drives Put response flags; creation requires exactly one S3, Extended S3, HTTP endpoint, OpenSearch, Splunk, Redshift, Snowflake, or Iceberg destination with valid endpoint, bucket, and role configuration, S3, HTTP, Splunk, and Snowflake buffering hints validate as paired integer bounds, destination KMS configuration validates and propagates S3 SSE metadata, Direct PUT throughput hints validate and appear in source descriptions, and Kinesis-source streams validate and describe their source ARNs; destination descriptions expose stable IDs, required AWS buffering/compression/encryption defaults, redact HTTP access keys, and support exclusive-start pagination; Extended S3 backup validates, delivers a separately configured copy, and supports one-way enablement plus nested updates; HTTP endpoints pass records through the same validated processor pipeline, write processing-failure envelopes to S3, receive the AWS JSON request and headers with optional GZIP and common attributes, aggregate PutRecordBatch records into one request, require a matching uncompressed 200 JSON acknowledgment without redirects, and back up AllData, retryable failures, and permanent failures such as HTTP 413 to S3; decompression-enabled PutRecord and PutRecordBatch reject callers without trusted CloudWatch Logs provenance; destination updates merge fields, reject stale version IDs, and increment the stream version; enabled stream encryption cryptographically protects retained records and persistent HTTP, OpenSearch, Splunk, Snowflake, and Redshift work with local AWS-owned or customer-managed KMS keys and rejects unavailable or disabled keys; OpenSearch destinations validate AWS-shaped configuration, index processed JSON into local domains with stable IDs and time rotation, persistently buffer by interval or size, resume across restarts, retry only failed documents with bounded backoff and configured expiry, and back up AllDocuments plus AWS-shaped failed-document envelopes to S3; Splunk destinations send concatenated processed records to Raw or Event HEC endpoints with stable request channels, resolve enabled Secrets Manager hec_token credentials, poll indexer acknowledgments with configured timeouts, persist buffered and retry work across restarts, exclude acknowledgment waits from retry duration, back up AllEvents, and emit AWS-shaped splunk-failed envelopes with original arrival times; Redshift destinations validate AWS-shaped JDBC, COPY, credentials, retry, staging, and backup configuration, redact passwords, stage processed records in S3, load declared tables through the local COPY row store, resolve rotating Secrets Manager username/password credentials, and persist encrypted five-minute COPY retries across restarts; the row store is not a PostgreSQL/Redshift SQL server; Snowflake destinations validate AWS-shaped account, table, credentials, role, buffering, retry, and S3 backup configuration, redact private keys and passphrases, load JSON mapping or VARIANT content and metadata into a persistent local row store, resolve rotating Secrets Manager user/private_key credentials, persist encrypted buffers across restarts, back up AllData, and emit AWS-shaped snowflake-failed envelopes; the row store is not a Snowflake SQL service and makes no Snowpipe network calls; Iceberg destinations validate catalog, table, role, S3, buffering, retry, and backup configuration, route processed JSON into local S3 Tables, apply insert, update, and delete operations using configured unique keys, enforce append-only streams, back up AllData, and emit failure envelopes; the row store does not produce Iceberg files or manifests; HTTP endpoints resolve enabled Secrets Manager api_key credentials through the local secret pack without exposing either key in descriptions; HTTP delivery persistently buffers blob-backed records until the configured interval or size hint, aggregates puts in arrival order, resumes across restarts, preserves puts arriving during in-flight requests, and cleans stream-scoped work on deletion; retryable failures likewise resume with the same request ID, updated destination credentials, bounded exponential backoff, configured-duration expiry, and S3 backup. CloudFront stores distribution/invalidation records — no CDN edge. EventBridge rules execute persistent default-bus rate/cron schedules, deliver to local SQS, SNS, Lambda, Step Functions, and API Destination targets, and persist retriable failures with bounded backoff, Retry-After timing, and standard-SQS dead-letter metadata; permanent failures go directly to the DLQ. EventBridge Scheduler executes at/rate/cron schedules in named time zones, invokes local SQS, SNS, Lambda, and asynchronous Step Functions templated targets, disperses flexible windows deterministically, and persists bounded retries with standard-SQS dead-letter delivery. Universal targets and other templated target types are not implemented.\n")
	sb.WriteString("OpenSearch is domain records plus an in-memory document store (match_all/term/match/query_string); not a Lucene cluster. Glue is catalog/job/crawler records — StartJobRun marks SUCCEEDED, no Spark. Athena scans Glue table Location on S3 as CSV or JSON lines and supports CREATE TABLE, INSERT VALUES, SELECT, and CTAS against `s3tablescatalog/<bucket>`; not Presto/Trino — no joins, aggregates, partitions, or Iceberg files. CloudTrail stores trails; LookupEvents is the in-memory log, not a real account feed. Organizations SCPs enforce action/resource statements across root, OU, and account attachments, with live and simulated Region, request-tag, IAM policy ARN, and EC2 IMDS token condition context; other service-specific context is limited.\n")
	sb.WriteString("Transfer/WAFv2/AppConfig/CodeBuild/Batch/EMR/MSK/Backup/Cognito Identity/SESv2 are control-plane records. No FTP daemon, packet inspection, AppConfig agent, Docker builds, YARN, Kafka broker, snapshot copy, or real IdP. Cognito Identity GetCredentialsForIdentity returns local keys.\n")
	sb.WriteString("Config stores recorders, rules, and PutEvaluations — no account crawler. X-Ray stores PutTraceSegments and retrieves them; no sampling daemon. GuardDuty stores detectors, IP sets, filters, members, and sample findings — no threat intel feed. Amazon MQ stores broker/user/config records — no ActiveMQ or RabbitMQ process. DocumentDB stores cluster/instance records — no MongoDB engine. IoT stores things, policies, rules, jobs, and fake certificates — no MQTT broker. EventBridge Pipes polls SQS plus single-shard Kinesis and DynamoDB Streams sources with persistent checkpoints, filters and transforms records, supports local Lambda, synchronous Express Step Functions, API Gateway, or API Destination enrichment with dynamic HTTP parameters, batches Lambda delivery, handles Lambda partial-batch responses, and targets local SQS/SNS/Lambda/Step Functions/API Destinations with synchronous Express or asynchronous invocation. API Destinations support API-key, basic, and OAuth client-credentials connections with 401/407 token refresh, connection header/query/body parameter precedence, persistent per-destination invocation-rate limits, and Retry-After propagation; no private endpoints or token cache. Pipes stream sources persist retry attempts, discard over-age or exhausted records, and route them to configured standard SQS/SNS DLQs; no MQ/Kafka sources or stream parallelization/bisect. CodePipeline stores pipelines; StartPipelineExecution marks Succeeded with no CodeBuild/CodeDeploy. AppSync stores GraphQL APIs and a `{ hello }` / `__typename` executor — no VTL. API Gateway v2 stores HTTP/WebSocket API records — no WebSocket fanout.\n")
	sb.WriteString("CodeCommit stores repositories, branches, and files — no git daemon. CodeDeploy stores applications, groups, and deployments — no fleet agent. Amplify stores apps, branches, and jobs — StartJob marks SUCCEED, no hosting. Inspector stores assessment targets/templates/runs — no agent scan. Security Hub stores hub enablement, findings, and insights — no aggregator.\n")
	sb.WriteString("Cognito InitiateAuth/AdminInitiateAuth is USER_PASSWORD_AUTH and ADMIN_* password flows against stored passwords; AccessToken/IdToken are HS256 JWTs signed with a seed-derived local key (not Cognito RSA JWKS). iat is the 2020-01-01 epoch so tokens are seed-stable. No SRP. Neptune stores cluster/instance records — no Gremlin or SPARQL engine.\n")
	sb.WriteString("Timestream stores databases, tables, and WriteRecords; Query is a table-name substring scan, not SQL. QLDB stores ledgers; SendCommand issues session/txn ids and appends statement text — not PartiQL and not a hash-chained journal.\n")
	sb.WriteString("DMS stores replication instances, endpoints, and tasks — StartReplicationTask flips status, no CDC. MediaConvert stores queues/templates/jobs; CreateJob is COMPLETE immediately, no transcode. Elasticsearch Service (aws.elasticsearch) is legacy domain records distinct from OpenSearch (aws.es); no Lucene cluster.\n")
	sb.WriteString("Elastic Beanstalk stores applications, versions, and environments — no EC2/ASG deploy. SWF stores domains, types, and executions; PollForActivityTask is empty (no workers). EFS stores file systems, mount targets, and access points — no NFS server.\n")
	sb.WriteString("Glacier stores vaults, archives, and jobs — InitiateJob is Succeeded immediately, no retrieval delay. Cloud Map stores namespaces/services/instances — no DNS. RAM stores resource shares — no cross-account grant. SageMaker stores notebooks, models, and endpoints; CreateEndpoint is InService immediately, no training or inference.\n")
	sb.WriteString("WorkSpaces stores workspace records — no VDI or directory. Transcribe jobs are COMPLETED immediately, no ASR. Rekognition collections store face ids; Detect* returns canned labels, no CV. Comprehend DetectSentiment is canned POSITIVE, no NLP. MediaStore stores container records — no origin. Kinesis Analytics StartApplication is RUNNING, no streaming SQL.\n")
	sb.WriteString("TranslateText echoes the input — no machine translation. Textract DetectDocumentText returns a canned LINE block, no OCR. Polly SynthesizeSpeech returns empty MPEG bytes, no TTS. FSx stores file system and backup records — no Lustre or Windows file server.\n")
	sb.WriteString("S3 Control stores access points and public-access-block records — no account S3 enforcement. Route 53 Resolver stores endpoints and rules — no DNS forwarding. Service Catalog stores products and portfolios — no provisioning. Shield stores protections — no DDoS mitigation.\n")
	sb.WriteString("WAF classic stores WebACL and IP set records — no packet inspection (distinct from WAFv2). Storage Gateway stores gateway and share records — no NFS or iSCSI. Lake Formation stores settings, grants, and registered resources — no Glue enforcement. Connect stores instance and user records — no contact center.\n")
	sb.WriteString("Pinpoint stores apps and campaigns; SendMessages is accepted with no delivery. DAX stores cluster records — no DynamoDB accelerator. MemoryDB stores cluster and user records — no Redis engine. Keyspaces stores keyspace and table records — no Cassandra CQL.\n")
	sb.WriteString("MWAA stores Airflow environment records — no scheduler or workers. SSO Admin stores permission sets and assignments — no IdP login. ACM PCA issues local untrusted PEMs. Lightsail stores instance and static-IP records — no VMs. Location SearchPlaceIndexForText is a canned point, no map tiles. Kendra Query returns empty hits, no search index. QuickSight stores datasets and dashboards — no BI engine.\n")
	sb.WriteString("Identity Store stores users and groups — no IdP directory. WorkMail stores organizations and users — no mail delivery. Direct Connect stores connection and LAG records — no physical circuits. Directory Service stores directory records — no AD or LDAP. GameLift stores fleets and sessions — no game servers. Forecast stores datasets and predictors — no ML. Personalize stores dataset groups and solutions — no recommendations. Lex Models stores bots and intents — no NLU runtime.\n")
	sb.WriteString("MediaLive stores channel and input records — no live transcode. MediaPackage stores channels and origin endpoints — no packaging. MediaConnect stores flows — no media transport. Elastic Transcoder CreateJob is Complete immediately, no transcode. CloudHSM v2 stores cluster records — no HSM hardware. Macie stores session and job records — no S3 content scan. Access Analyzer stores analyzer records — no IAM policy analysis.\n")
	sb.WriteString("Comprehend Medical DetectEntitiesV2 returns a canned MEDICAL_CONDITION, no clinical model. Fraud Detector stores detector records — no ML scoring. App Mesh stores meshes and virtual nodes — no Envoy dataplane. HealthLake stores FHIR datastore records — no FHIR server. Lookout for Metrics stores detectors and alerts — no anomaly ML.\n")
	sb.WriteString("Bedrock stores guardrail records — ListFoundationModels is canned, no inference. FIS stores experiment templates — StartExperiment is completed with no injection. Cost Explorer GetCostAndUsage is a canned zero Amount, no CUR. Resource Groups stores group records — no resource query. Verified Permissions stores policy stores — no Cedar evaluation. Support stores cases — ResolveCase drops the local record, no AWS Support backend. CodeArtifact stores domain and repository records — no artifact registry.\n")
	sb.WriteString("Cloud Control stores resource records — CreateResource is SUCCESS immediately, no CloudFormation handler. Serverless Application Repository stores application records — no SAR deploy. Account stores alternate contacts — no org account mutation. IoT Wireless stores destinations and devices — no LoRaWAN. S3 Tables stores table-bucket records and local schema-aware row mutations — no Iceberg file or manifest engine.\n")
	sb.WriteString("Synthetics stores canary records — StartCanary does not run a browser. App Runner stores service records — no container deploy. Proton stores environment records — no IaC provision. Resilience Hub stores app records — no scoring. Resource Explorer stores index records — no account crawl. RUM PutRumEvents is accepted with no analytics. EventBridge Schemas stores registry records — no discovery. Aurora DSQL stores cluster records — no SQL engine. CodeConnections stores connection records — no git OAuth. IoT Data stores thing shadows — no MQTT broker. Managed Blockchain stores network records — no Fabric or Ethereum. Kinesis Analytics v2 / Flink stores application records — StartApplication does not run Flink (distinct from v1).\n")
	return sb.String()
}
