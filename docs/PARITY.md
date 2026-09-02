# Parity audit

This ledger separates operation routing, line coverage, test forms, and behavioral parity. None is a substitute for another.

## S3 baseline

Authority: LocalStack commit `c2cb02372f48cde90b06f0e6ce809a058251fbd7`, audited on 2026-09-02.

| Measure | Current evidence |
|---|---:|
| Requested test forms wired | 7 / 7 (100%) |
| S3 operations routed to emulation | 115 / 115 (100%) |
| Whole-repository statement coverage | 83.9% |
| S3 statement coverage | 89.8% |
| LocalStack S3 test functions explicitly traced | 108 / 463 (23.3%) |
| LocalStack S3 test functions not yet traced | 355 / 463 (76.7%) |

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
| `test_s3_list_operations.py::TestS3ListBuckets::test_list_buckets_by_prefix_with_case_sensitivity` | `TestListBucketsPaginationAndFilters` verifies uppercase and lowercase prefixes are distinct and the requested prefix is returned | Mapped and green |
| `test_s3_list_operations.py::TestS3ListBuckets::test_list_buckets_with_max_buckets` | `TestListBucketsPaginationAndFilters`, SDK paginator contract, HTTP BDD, fuzz, snapshot, and mutation coverage verify bounded pages and opaque continuation tokens | Mapped and green |
| `test_s3_list_operations.py::TestS3ListBuckets::test_list_buckets_when_continuation_token_is_empty` | `TestListBucketsPaginationAndFilters` verifies an explicit empty token starts at the first matching bucket and still returns pagination metadata | Mapped and green |
| `test_s3_list_operations.py::TestS3ListBuckets::test_list_buckets_by_bucket_region` | `TestListBucketsPaginationAndFilters` and the SDK contract verify exact region filtering and `BucketRegion` output | Mapped and green |
| `test_s3_list_operations.py::TestS3ListBuckets::test_list_buckets_with_continuation_token` | `TestListBucketsPaginationAndFilters`, SDK paginator contract, HTTP BDD, fuzz, snapshot, and mutation coverage verify distinct ordered pages | Mapped and green |
| `test_s3_list_operations.py::TestS3ListBuckets::test_list_buckets_region_validation` | `TestListBucketsPaginationAndFilters` verifies the exact `InvalidArgument`, message, status, and `bucket-region` field for an unknown AWS region | Mapped and green |
| `test_s3_list_operations.py::TestS3ListBuckets::test_region_validation_non_standard_regions_enabled` | `TestListBucketsPaginationAndFilters` verifies opt-in creation and filtering in a nonstandard region plus cross-region endpoint rejection; config env/file unit and mutation coverage pin the opt-in | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjects::test_list_objects_with_prefix` | `TestListObjectsDelimiterEncodingAndEmptyMarker` verifies empty, slash, double-encoded `%2F`, and raw `%2F` delimiters with prefix and URL-encoding behavior | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjects::test_list_objects_next_marker` | `TestListObjectsPaginationIncludesCommonPrefixes` verifies V1 next-marker presence with a delimiter, omission without one, and ordered continuation | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjects::test_s3_list_objects_empty_marker` | `TestListObjectsDelimiterEncodingAndEmptyMarker` verifies an explicit empty marker on an empty bucket returns an empty first page | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjects::test_list_objects_marker_common_prefixes` | `TestListObjectsPaginationIncludesCommonPrefixes` and characterization snapshots verify common-prefix, object, terminal, and manual-marker pages | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjects::test_s3_list_objects_timestamp_precision` | `TestListObjectsV2Prefix` and the pagination characterization verify V1/V2 timestamps retain S3 millisecond precision; a semantic mutant rejects RFC3339 values without `.000Z` | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectsV2::test_list_objects_v2_with_prefix` | `TestListObjectsV2Prefix`, raw HTTP prefix routing in `TestListObjectsDelimiterEncodingAndEmptyMarker`, SDK contract, BDD, and fuzz coverage verify nested prefix selection | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectsV2::test_list_objects_v2_with_prefix_and_delimiter` | `TestListObjectsPaginationIncludesCommonPrefixes`, `TestListURLResponseEncoding`, characterization snapshots, and mutation coverage verify prefix/delimiter pages and tokens | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectsV2::test_list_objects_v2_continuation_start_after` | `TestListObjectsV2OpaqueContinuationTokens`, `TestListObjectsV2SafeContinuationTokens`, `TestListURLResponseEncoding`, fuzz, and mutation coverage verify continuation precedence, `StartAfter`, truncation, and empty-token rejection | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectsV2::test_list_objects_v2_continuation_common_prefixes` | `TestListObjectsPaginationIncludesCommonPrefixes` and characterization snapshots verify common-prefix, object, and terminal continuation pages | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectsV2::test_list_objects_v2_continuation_token_safe_chars` | `TestListObjectsV2SafeContinuationTokens` round-trips percent signs, spaces, at signs, slashes, equals signs, and Unicode across pages and URL-encoded `StartAfter`; fuzz and opaque-token mutations cover the shared cursor logic | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectsV2::test_list_ops_encoding_type_validation` | `TestListEncodingTypeValidation`, characterization snapshots, fuzz, and mutation coverage verify missing/`url` success plus wrong and empty values across all four list operations | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectVersions::test_list_objects_versions_markers` | `TestListObjectVersionsPagination` verifies key-only, version-only rejection, combined markers, latest ordering, delete markers, and terminal pages with mutation coverage | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectVersions::test_list_object_versions_pagination_common_prefixes` | `TestListObjectVersionsPagination` verifies common-prefix and subsequent object pages, while URL-marker characterization covers encoded marker round trips | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectVersions::test_list_objects_versions_with_prefix` | `TestListObjectVersionsPagination`, `TestListObjectVersionsURLResponseEncoding`, raw HTTP list routing, fuzz, and mutation coverage verify prefix/delimiter selection | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectVersions::test_list_objects_versions_with_prefix_only_and_pagination` | `TestListObjectVersionsPagination` verifies five same-key versions across key/version markers, key-only continuation, and exact terminal counts | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectVersions::test_list_objects_versions_with_prefix_only_and_pagination_many_versions` | `TestListObjectVersionsManyVersionsPagination` verifies the pinned 101-version boundary produces pages of 100 and 1 with exact markers | Mapped and green |
| `test_s3_list_operations.py::TestS3ListObjectVersions::test_s3_list_object_versions_timestamp_precision` | `TestListObjectVersionsPagination` verifies version timestamps use S3 millisecond precision; the same formatter covers delete markers and is mutation-tested | Mapped and green |
| `test_s3_list_operations.py::TestS3ListMultipartUploads::test_list_multiparts_next_marker` | `TestListMultipartUploadsPaginationAndDelimiter` verifies empty/all listings, same-key initiation ordering, key-only and upload-only markers, combined markers, mismatched IDs, truncation, and terminal pages | Mapped and green |
| `test_s3_list_operations.py::TestS3ListMultipartUploads::test_list_multiparts_with_prefix_and_delimiter` | `TestListMultipartUploadsPaginationAndDelimiter`, URL-encoding unit coverage, and characterization snapshots verify nested prefix/delimiter selection and raw routing | Mapped and green |
| `test_s3_list_operations.py::TestS3ListMultipartUploads::test_list_multipart_uploads_marker_common_prefixes` | `TestListMultipartUploadsPaginationAndDelimiter` and characterization snapshots verify unpageable common-prefix pages plus manual prefix and object-key markers | Mapped and green |
| `test_s3_list_operations.py::TestS3ListMultipartUploads::test_s3_list_multiparts_timestamp_precision` | `TestListMultipartUploadsPaginationAndDelimiter` verifies S3 millisecond initiation timestamps; focused mutation coverage pins the shared conversion | Mapped and green |
| `test_s3_list_operations.py::TestS3ListParts::test_list_parts_pagination` | `TestListPartsAndMultipartUploads` and `TestListPartsCharacterization` verify empty/all, first/next/beyond-final pages, ordered part numbers, checksums, and exact markers | Mapped and green |
| `test_s3_list_operations.py::TestS3ListParts::test_list_parts_empty_part_number_marker` | `TestMultipartZeroLimitsUseDefaults` verifies raw HTTP empty `part-number-marker` and `max-parts` values use zero/default semantics without rejection | Mapped and green |
| `test_s3_list_operations.py::TestS3ListParts::test_s3_list_parts_timestamp_precision` | `TestListPartsAndMultipartUploads` and characterization snapshots verify S3 millisecond `LastModified` precision with semantic mutation coverage | Mapped and green |
| `test_s3_list_operations.py::TestS3ListParts::test_list_parts_via_object_attrs_pagination` | `TestGetObjectAttributesContract` verifies three checksum-bearing completed parts across first, next, and beyond-final object-attributes pages, including totals and markers | Mapped and green |
| `test_s3_api.py::TestS3BucketCRUD::test_delete_bucket_with_objects` | `TestDeleteBucketRequiresEmptyBucket` verifies the exact unversioned `BucketNotEmpty` fault, preserves the object after rejection, and deletes the bucket after the object is removed | Mapped and green |
| `test_s3_api.py::TestS3BucketCRUD::test_delete_versioned_bucket_with_objects` | `TestDeleteBucketRequiresEmptyBucket` verifies version history and a lone delete marker each block deletion with the versioned AWS message, then deletes the bucket after the marker is removed | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_delete_object` | `TestDeleteObjectRejectsVersionOnUnversionedMissingKey` and `TestDeleteObjectMissingKeyVersionIsIdempotent` verify idempotent unversioned deletion plus exact invalid-version handling, with snapshot and mutation coverage | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_delete_objects` | `TestDeleteObjectsVersionAndQuietSemantics` verifies an unversioned wrong version returns `NoSuchVersion` while existing and missing keys are all reported as deleted | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_delete_object_versioned` | `TestDeleteObjectRestoresPreviousVersion`, `TestGetObjectVersionErrors`, SDK contract, HTTP BDD, snapshot, fuzz, and mutation coverage verify markers, explicit version deletion, restoration, and exact `NoSuchVersion` reads | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_delete_objects_versioned` | `TestDeleteObjectsVersionAndQuietSemantics`, SDK contract, HTTP BDD, fuzz, chaos, and mutation coverage verify markers, explicit versions, missing-version errors, quiet responses, and restoration | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_get_object_with_version_unversioned_bucket` | `TestGetObjectVersionErrors`, SDK contract, HTTP BDD, snapshot, fuzz, and five focused mutants verify `versionId=null` reads the current object while other version IDs return exact `InvalidArgument` details | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_put_object_on_suspended_bucket` | `TestSuspendedVersioningReplacesNullVersion`, SDK contract, HTTP BDD, fuzz, chaos, snapshot, and mutation coverage verify one replaceable null version alongside preserved enabled versions | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_delete_object_on_suspended_bucket` | `TestSuspendedVersioningReplacesNullVersion`, SDK contract, HTTP BDD, fuzz, chaos, snapshot, and mutation coverage verify null delete-marker replacement and restoration of the prior enabled version | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_list_object_versions_order_unversioned` | `TestListObjectVersionsUnversionedOrder` verifies unversioned objects are key-ordered, latest, and represented with the null version ID; list pagination and mutation coverage exercise the shared implementation | Mapped and green |
| `test_s3_api.py::TestS3ObjectCRUD::test_get_object_range` | `TestObjectByteRanges`, SDK contract, HTTP BDD, fuzz, and mutation coverage verify bounded/open/suffix/ignored ranges, checksum handling, 206 metadata, and exact 416 size/request fields | Mapped and green |
| `test_s3_api.py::TestS3BucketVersioning::test_bucket_versioning_crud` | `TestBucketVersioningState`, SDK contract, HTTP BDD, snapshot, fuzz, and mutation coverage verify unset, suspended, enabled, invalid, empty, and missing-bucket behavior with the exact REST-XML faults | Mapped and green |
| `test_s3_api.py::TestS3BucketVersioning::test_object_version_id_format` | `TestBucketVersioningState`, HTTP BDD, fuzz, snapshots, and two focused mutants verify object and delete-marker version IDs are 32 lowercase hexadecimal characters | Mapped and green |
| `test_s3_api.py::TestS3BucketEncryption::test_s3_default_bucket_encryption` | `TestBucketEncryptionConfiguration`, HTTP spine/BDD, SDK contract, snapshot, and mutation coverage verify the fresh and post-delete AES256 default plus idempotent deletion | Mapped and green |
| `test_s3_api.py::TestS3BucketEncryption::test_s3_default_bucket_encryption_exc` | `TestBucketEncryptionConfiguration`, REST-XML contract, BDD, fuzz, snapshot, and mutation coverage verify missing buckets, invalid rule counts, and exact malformed/invalid-key faults without replacing valid state | Mapped and green |
| `test_s3_api.py::TestS3BucketEncryption::test_s3_bucket_encryption_sse_s3` | `TestBucketEncryptionConfiguration`, SDK contract, HTTP BDD, fuzz, and snapshots verify AES256 inheritance across writes and reads while suppressing inapplicable bucket-key headers | Mapped and green |
| `test_s3_api.py::TestS3BucketEncryption::test_s3_bucket_encryption_sse_kms` | `TestExplicitKMSKeyValidation`, `TestBucketEncryptionConfiguration`, SDK contract, BDD, fuzz, chaos, snapshot, and mutation coverage verify canonical KMS ARNs, enabled-state validation, inherited headers, and bucket-key toggles | Mapped and green |
| `test_s3_api.py::TestS3BucketEncryption::test_s3_bucket_encryption_sse_kms_aws_managed_key` | `TestExplicitKMSKeyValidation` and `TestConcurrentManagedS3KMSKeyCreation` verify one reusable `alias/aws/s3` key with AWS-managed metadata is materialized, returned by S3, and discoverable through KMS under concurrent writes; fuzz and mutation coverage pin reuse | Mapped and green |

## Source-only findings

These fixes are verified against pinned LocalStack implementation paths but do not increase the test-function numerator:

| LocalStack source | Mirror evidence |
|---|---|
| `validate_failed_precondition` exact ETag comparison | PR #229 |
| `get_failed_precondition_copy_source` exact ETag comparison | PR #229 |
| `get_failed_upload_part_copy_source_preconditions` exact ETag comparison | PR #229 |

Operation support is complete at the routing layer. Behavioral parity is not complete or yet quantifiable beyond the explicit 108/463 lower bound; each remaining test function must be mapped, reproduced when divergent, and covered before the parity audit can reach 100%.
