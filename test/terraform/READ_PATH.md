# Terraform refresh read-path

A test asserts none of these return HTTP 501.

## aws_s3_bucket

- GetBucketAcl
- GetBucketPolicy
- GetBucketCors
- GetBucketWebsite
- GetBucketVersioning
- GetBucketLogging
- GetBucketLifecycleConfiguration
- GetBucketReplication
- GetBucketEncryption
- GetBucketObjectLockConfiguration
- GetBucketRequestPayment
- GetBucketAccelerateConfiguration
- GetBucketTagging
- GetBucketNotificationConfiguration
- GetBucketLocation

## aws_dynamodb_table

- DescribeTable
- DescribeTimeToLive
- DescribeContinuousBackups
- ListTagsOfResource

## aws_sqs_queue

- GetQueueAttributes
- ListQueueTags
