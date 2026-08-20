// Package catalog holds hand-built canonical models for v1 emulate-tier
// services so the spine boots before specs-sync. S1 replaces these with
// generated models from vendored specs.
package catalog

import "github.com/tyler-r-kendrick/mirror.cloud/internal/model"

func op(name, method, uri string, code int, ro bool) model.Operation {
	if code == 0 {
		code = 200
	}
	return model.Operation{
		Name:        name,
		HTTP:        model.HTTPBinding{Method: method, URI: uri, Code: code},
		Target:      name,
		QueryAction: name,
		Confidence:  model.ConfDeclared,
		Readonly:    ro,
	}
}

func ops(list ...model.Operation) []model.Operation { return list }

func svc(id, prefix string, proto model.Protocol, target, ver, xmlns string, o []model.Operation) model.Service {
	return model.Service{
		ID:             id,
		Protocol:       proto,
		EndpointPrefix: prefix,
		TargetPrefix:   target,
		QueryVersion:   ver,
		XMLNamespace:   xmlns,
		Operations:     o,
		Shapes:         map[string]model.Shape{},
	}
}

// Bundle returns the v1 emulate-tier + GCS model.
func Bundle() *model.Bundle {
	s3ops := []model.Operation{}
	for _, n := range []string{
		"CreateBucket", "DeleteBucket", "HeadBucket", "ListBuckets", "GetBucketLocation",
		"GetBucketVersioning", "PutBucketVersioning", "GetBucketTagging", "PutBucketTagging",
		"GetBucketNotificationConfiguration", "PutBucketNotificationConfiguration",
		"GetBucketAcl", "GetBucketPolicy", "GetBucketCors", "GetBucketWebsite",
		"GetBucketLogging", "GetBucketLifecycleConfiguration", "GetBucketReplication",
		"GetBucketEncryption", "GetBucketObjectLockConfiguration", "GetBucketRequestPayment",
		"GetBucketAccelerateConfiguration",
		"PutObject", "GetObject", "HeadObject", "DeleteObject", "DeleteObjects", "CopyObject",
		"ListObjects", "ListObjectsV2", "ListObjectVersions",
		"CreateMultipartUpload", "UploadPart", "UploadPartCopy", "CompleteMultipartUpload",
		"AbortMultipartUpload", "ListParts", "ListMultipartUploads",
		"GetObjectTagging", "PutObjectTagging",
	} {
		s3ops = append(s3ops, op(n, "POST", "/", 200, stringsHas(n, "Get", "Head", "List")))
	}
	ddb := []string{"CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTable",
		"PutItem", "GetItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem",
		"Query", "Scan", "TransactGetItems", "TransactWriteItems",
		"TagResource", "UntagResource", "ListTagsOfResource",
		"DescribeTimeToLive", "DescribeContinuousBackups"}
	sqs := []string{"CreateQueue", "DeleteQueue", "GetQueueUrl", "ListQueues", "GetQueueAttributes",
		"SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage",
		"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility",
		"ChangeMessageVisibilityBatch", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags"}
	sns := []string{"CreateTopic", "DeleteTopic", "ListTopics", "GetTopicAttributes", "SetTopicAttributes",
		"Subscribe", "ConfirmSubscription", "Unsubscribe", "ListSubscriptions",
		"ListSubscriptionsByTopic", "Publish", "PublishBatch", "TagResource", "UntagResource"}
	sts := []string{"GetCallerIdentity", "AssumeRole", "GetSessionToken", "GetFederationToken"}
	iam := []string{"CreateRole", "GetRole", "UpdateRole", "DeleteRole", "ListRoles",
		"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
		"AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies",
		"CreatePolicy", "GetPolicy", "DeletePolicy", "ListPolicies",
		"CreateUser", "GetUser", "DeleteUser", "ListUsers", "CreateAccessKey",
		"TagRole", "UntagRole"}
	ssm := []string{"PutParameter", "GetParameter", "GetParameters", "GetParametersByPath",
		"DeleteParameter", "DeleteParameters", "DescribeParameters", "LabelParameterVersion",
		"GetParameterHistory", "AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource"}
	sm := []string{"CreateSecret", "GetSecretValue", "PutSecretValue", "UpdateSecret", "DeleteSecret",
		"RestoreSecret", "ListSecrets", "DescribeSecret", "ListSecretVersionIds",
		"GetRandomPassword", "TagResource", "UntagResource"}
	gcs := []string{"storage.buckets.insert", "storage.buckets.get", "storage.buckets.list",
		"storage.buckets.delete", "storage.buckets.patch",
		"storage.objects.insert", "storage.objects.get", "storage.objects.list",
		"storage.objects.delete", "storage.objects.copy", "storage.objects.rewrite",
		"storage.objects.compose", "storage.objects.patch"}

	mk := func(names []string) []model.Operation {
		o := make([]model.Operation, 0, len(names))
		for _, n := range names {
			o = append(o, op(n, "POST", "/", 200, stringsHas(n, "Get", "Describe", "List", "Head")))
		}
		return o
	}

	return &model.Bundle{
		SchemaVersion: "1",
		Provider:      model.ProviderAWS,
		Services: []model.Service{
			svc("aws.s3", "s3", model.ProtoRESTXML, "", "", "http://s3.amazonaws.com/doc/2006-03-01/", s3ops),
			svc("aws.dynamodb", "dynamodb", model.ProtoAWSJSON10, "DynamoDB_20120810", "", "", mk(ddb)),
			svc("aws.sqs", "sqs", model.ProtoAWSJSON10, "AmazonSQS", "2012-11-05", "", mk(sqs)),
			svc("aws.sns", "sns", model.ProtoAWSQuery, "", "2010-03-31", "http://sns.amazonaws.com/doc/2010-03-31/", mk(sns)),
			svc("aws.sts", "sts", model.ProtoAWSQuery, "", "2011-06-15", "https://sts.amazonaws.com/doc/2011-06-15/", mk(sts)),
			svc("aws.iam", "iam", model.ProtoAWSQuery, "", "2010-05-08", "https://iam.amazonaws.com/doc/2010-05-08/", mk(iam)),
			svc("aws.ssm", "ssm", model.ProtoAWSJSON11, "AmazonSSM", "", "", mk(ssm)),
			svc("aws.secretsmanager", "secretsmanager", model.ProtoAWSJSON11, "secretsmanager", "", "", mk(sm)),
			svc("gcp.storage", "storage", model.ProtoGCPRESTSON, "", "", "", mk(gcs)),
		},
	}
}

func stringsHas(n string, prefixes ...string) bool {
	for _, p := range prefixes {
		if len(n) >= len(p) && n[:len(p)] == p {
			return true
		}
	}
	return false
}
