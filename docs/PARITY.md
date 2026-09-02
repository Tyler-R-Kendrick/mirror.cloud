# Parity audit

This ledger separates operation routing, line coverage, test forms, and behavioral parity. None is a substitute for another.

## S3 baseline

Authority: LocalStack commit `c2cb02372f48cde90b06f0e6ce809a058251fbd7`, audited on 2026-09-02.

| Measure | Current evidence |
|---|---:|
| Requested test forms wired | 7 / 7 (100%) |
| S3 operations routed to emulation | 115 / 115 (100%) |
| Whole-repository statement coverage | 83.9% |
| S3 statement coverage | 89.6% |
| LocalStack S3 test functions explicitly traced | 58 / 463 (12.5%) |
| LocalStack S3 test functions not yet traced | 405 / 463 (87.5%) |

The traceability percentage is intentionally a lower bound. Historical Mirror tests and implementations do not count until a pinned LocalStack test function has an explicit evidence row below. Parametrized cases are not expanded in the denominator, so this measures direct test-function review rather than pytest case count.

## Pinned inventory

| File | Direct test functions |
|---|---:|
| `test_s3.py` | 282 |
| `test_s3_api.py` | 93 |
| `test_s3_concurrency.py` | 4 |
| `test_s3_cors.py` | 19 |
| `test_s3_list_operations.py` | 32 |
| `test_s3_notifications_eventbridge.py` | 5 |
| `test_s3_notifications_lambda.py` | 3 |
| `test_s3_notifications_sns.py` | 4 |
| `test_s3_notifications_sqs.py` | 18 |
| `test_s3_preconditions.py` | 3 |
| **Total** | **463** |

## Traced tests

| LocalStack test | Mirror evidence | Result |
|---|---|---|
| `test_s3.py::TestS3::test_s3_get_object_preconditions` | `TestObjectReadConditionsCharacterization`, AWS SDK contract, HTTP BDD, fuzz, chaos, mutation; PRs #229-#231 | Mapped and green |
| `test_s3.py::TestS3::test_precondition_failed_error` | `TestObjectReadConditionsCharacterization`, AWS SDK contract, HTTP BDD, snapshot, mutation; PR #230 | Mapped and green |
| `test_s3_preconditions.py::test_s3_copy_object_preconditions` | `TestCopySourcePreconditionsCharacterization`, AWS SDK contract, HTTP BDD, fuzz, chaos, mutation | Mapped and green |
| `test_s3_preconditions.py::test_s3_copy_object_if_source_modified_since_versioned` | Versioned characterization and contract boundary checks, HTTP BDD, fuzz, chaos, mutation | Mapped and green |
| `test_s3_preconditions.py::test_s3_copy_object_if_source_unmodified_since_versioned` | Versioned characterization and contract boundary checks, HTTP BDD, fuzz, chaos, mutation | Mapped and green |
| `test_s3_concurrency.py::TestParallelBucketCreation::test_parallel_bucket_creation` | `TestConcurrentCrossRegionBucketsPaginateWithoutAccountLeaks` exercises parallel distinct buckets and us-east-1 idempotent recreation; collision unit, snapshot, fuzz, and mutation coverage | Mapped and race-clean |
| `test_s3_concurrency.py::TestParallelBucketCreation::test_parallel_object_creation_and_listing` | `TestConcurrentListObjectPaginationRemainsOrdered`, list pagination characterization, AWS SDK contract, HTTP BDD, fuzz, and mutation coverage | Mapped and race-clean |
| `test_s3_concurrency.py::TestParallelBucketCreation::test_parallel_object_creation_and_read` | `TestConcurrentPutsAndGetsSameKey`, object round-trip unit, AWS SDK contract, and HTTP BDD coverage | Mapped and race-clean |
| `test_s3_concurrency.py::TestParallelBucketCreation::test_parallel_object_read_range` | `TestConcurrentPutsAndGetsSameKey`, `TestObjectByteRanges`, AWS SDK contract, HTTP BDD, and mutation coverage | Mapped and race-clean |
| `test_s3_cors.py::TestS3Cors::test_cors_http_options_no_config` | `TestBucketCorsHTTP`, `TestBucketCorsCharacterization`, HTTP BDD, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_http_get_no_config` | `TestBucketCorsHTTP` unconfigured actual-request checks and HTTP BDD coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_no_config_localstack_allowed` | `TestBucketCorsHTTP` and characterization coverage for LocalStack default origins | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_list_buckets` | `TestBucketCorsHTTP` preflight and actual-request checks for `ListBuckets` | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_http_options_non_existent_bucket` | `TestBucketCorsHTTP` missing-origin, unconfigured, and missing-bucket checks | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_http_options_non_existent_bucket_ls_allowed` | `TestBucketCorsHTTP` missing-bucket LocalStack-default check | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_match_origins` | `TestBucketCorsHTTP`, characterization snapshot, SDK contract, BDD, fuzz, chaos, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_options_match_partial_origin` | `TestBucketCorsHTTP` wildcard-origin match and CORS fuzz coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_options_fails_partial_origin` | `TestBucketCorsHTTP` trailing-path rejection and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_match_methods` | `TestBucketCorsHTTP`, SDK contract, BDD, fuzz, chaos, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_match_headers` | `TestBucketCorsHTTP` case-insensitive wildcard and comma-separated header checks plus mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_cors_expose_headers` | `TestBucketCorsHTTP`, characterization snapshot, SDK contract, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_get_cors` | `TestBucketCors`, characterization snapshot, SDK contract, BDD, fuzz, chaos, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_put_cors` | `TestBucketCors`, characterization snapshot, SDK contract, BDD, fuzz, chaos, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_put_cors_default_values` | `TestBucketCors` optional-field round trip and HTTP preflight coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_put_cors_invalid_rules` | `TestBucketCors`, characterization snapshot, BDD, fuzz, chaos, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_put_cors_empty_origin` | `TestBucketCors` empty-origin round trip | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_delete_cors` | `TestBucketCors`, characterization snapshot, SDK contract, BDD, fuzz, and mutation coverage | Mapped and green |
| `test_s3_cors.py::TestS3Cors::test_s3_cors_disabled` | `TestS3AWSChunkedContentEncodingCharacterization`, SDK contract, HTTP BDD, and content-encoding mutation coverage; Mirror has no custom-CORS toggle in its decoding path | Mapped and green |
| `test_s3_notifications_eventbridge.py::TestS3NotificationsToEventBridge::test_object_created_put` | `TestBucketNotificationEventBridgeDelivery` verifies EventBridge-to-SQS create and delete delivery, distinct event types, and object details | Mapped and green |
| `test_s3_notifications_eventbridge.py::TestS3NotificationsToEventBridge::test_object_put_acl` | `TestBucketNotificationEventBridgeDelivery` verifies ACL update delivery and omits size/sequencer; restore/ACL record coverage also exists | Mapped and green |
| `test_s3_notifications_eventbridge.py::TestS3NotificationsToEventBridge::test_restore_object` | `TestBucketNotificationEventBridgeDelivery` verifies initiated/completed restore events, expiry, source storage class, and source-IP omission | Mapped and green |
| `test_s3_notifications_eventbridge.py::TestS3NotificationsToEventBridge::test_object_created_put_in_different_region` | `TestBucketNotificationEventBridgeDelivery` writes through a secondary-region identity and verifies the stored bucket region in every event | Mapped and green |
| `test_s3_notifications_eventbridge.py::TestS3NotificationsToEventBridge::test_object_created_put_versioned` | `TestBucketNotificationEventBridgeDelivery` verifies enabled/suspended puts, version IDs, delete markers, and permanent deletion events | Mapped and green |
| `test_s3_notifications_lambda.py::TestS3NotificationsToLambda::test_create_object_put_via_dynamodb` | `TestBucketNotificationLambdaDelivery` invokes a real Lambda function and verifies the delivered PUT record and object key; Mirror reads the function output directly instead of using DynamoDB as a test intermediary | Mapped and green |
| `test_s3_notifications_lambda.py::TestS3NotificationsToLambda::test_create_object_by_presigned_request_via_dynamodb` | `TestBucketNotificationLambdaDelivery` verifies PUT and multipart POST event delivery; `TestBootedServerS3VersioningPaginationPresign` covers presigned PUT through the HTTP edge and `TestPostObjectPolicySignatureCharacterization` covers signed browser POST policies | Mapped and green |
| `test_s3_notifications_lambda.py::TestS3NotificationsToLambda::test_invalid_lambda_arn` | `TestBucketNotificationConfiguration` verifies malformed and missing Lambda ARNs with destination validation enabled, malformed ARN rejection when skipped, and missing-destination persistence when skipped | Mapped and green |
| `test_s3_notifications_sns.py::TestS3NotificationsToSns::test_object_created_put` | `TestBucketNotificationTopicDelivery` verifies two S3 PUT records through SNS to SQS, including notification envelope type, topic, subject, event source/name, bucket, key, and distinct object sizes | Mapped and green |
| `test_s3_notifications_sns.py::TestS3NotificationsToSns::test_bucket_notifications_with_filter` | `TestBucketNotificationTopicDelivery` verifies a topic prefix filter excludes a nonmatching key and delivers both matching writes | Mapped and green |
| `test_s3_notifications_sns.py::TestS3NotificationsToSns::test_bucket_not_exist` | `TestBucketNotificationConfiguration` verifies PUT and GET notification configuration return `NoSuchBucket`, including when destination validation is skipped | Mapped and green |
| `test_s3_notifications_sns.py::TestS3NotificationsToSns::test_invalid_topic_arn` | `TestBucketNotificationConfiguration` verifies malformed and missing topic ARNs with validation enabled, malformed ARN rejection when skipped, and missing-destination persistence when skipped | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_created_put` | `TestBucketNotificationDeliveryFilters` verifies matching PUT delivery to SQS; `TestObjectCreatedEventNames` verifies the shared record name, key, and size | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_created_copy` | `TestObjectCreatedEventNames` verifies `ObjectCreated:Copy`, encoded object key, and copied size through the notification payload used by SQS | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_created_and_object_removed` | `TestBucketNotificationDeliveryFilters` and `TestBucketNotificationRemovalAndTaggingEvents` verify queue delivery for create, delete, and delete-marker events | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_delete_objects` | `TestBucketNotificationRemovalAndTaggingEvents` verifies one event per bulk-delete entry, including two nonexistent unversioned keys | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_created_complete_multipart_upload` | `TestObjectCreatedEventNames` verifies the complete-multipart event name, key, and assembled object size | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_key_encoding` | `TestObjectCreatedEventNames` verifies key `a@b` is emitted as `a%40b` | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_created_put_with_presigned_url_upload` | `TestBootedServerS3VersioningPaginationPresign` verifies presigned HTTP PUT dispatch; `TestBucketNotificationDeliveryFilters` verifies the same PUT dispatch publishes the queue record | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_tagging_put_event` | `TestBucketNotificationRemovalAndTaggingEvents` verifies `ObjectTagging:Put` queue delivery | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_tagging_delete_event` | `TestBucketNotificationRemovalAndTaggingEvents` verifies `ObjectTagging:Delete` queue delivery | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_xray_header` | `TestBucketNotificationDeliveryFilters` performs an HTTP PUT with `X-Amzn-Trace-Id` and verifies SQS exposes the matching `AWSTraceHeader` system attribute | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_notifications_with_filter` | `TestBucketNotificationDeliveryFilters` verifies combined prefix/suffix filtering and removal-event delivery; configuration round trips through unit, SDK contract, snapshot, fuzz, and mutation coverage | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_filter_rules_case_insensitive` | `TestBucketNotificationConfiguration` verifies case-insensitive filter names normalize to `Prefix`; delivery matching also accepts either case | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_filter_rules_empty_value` | `TestBucketNotificationConfiguration` verifies an empty suffix is accepted, normalized, stored, and returned when destination validation is skipped | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_invalid_sqs_arn` | `TestBucketNotificationConfiguration` verifies malformed and missing queue ARNs, malformed rejection with validation skipped, and missing-destination persistence when skipped | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_multiple_invalid_sqs_arns` | `TestBucketNotificationConfiguration` verifies multiple malformed queue configurations fail atomically and preserve the prior configuration; missing queue validation is covered separately | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_put_acl` | `TestBucketNotificationRestoreAndACLEvents` verifies `ObjectAcl:Put` queue delivery and record fields | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_restore_object` | `TestBucketNotificationRestoreAndACLEvents` verifies initiated and completed restore queue events, expiry, and storage class | Mapped and green |
| `test_s3_notifications_sqs.py::TestS3NotificationsToSQS::test_object_created_put_versioned` | `TestBucketNotificationRemovalAndTaggingEvents` verifies a versioned PUT queue record includes the exact version ID, alongside delete-marker and permanent-delete events | Mapped and green |

## Source-only findings

These fixes are verified against pinned LocalStack implementation paths but do not increase the test-function numerator:

| LocalStack source | Mirror evidence |
|---|---|
| `validate_failed_precondition` exact ETag comparison | PR #229 |
| `get_failed_precondition_copy_source` exact ETag comparison | PR #229 |
| `get_failed_upload_part_copy_source_preconditions` exact ETag comparison | PR #229 |

Operation support is complete at the routing layer. Behavioral parity is not complete or yet quantifiable beyond the explicit 58/463 lower bound; each remaining test function must be mapped, reproduced when divergent, and covered before the parity audit can reach 100%.
