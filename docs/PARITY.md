# Parity audit

This ledger separates operation routing, line coverage, test forms, and behavioral parity. None is a substitute for another.

## S3 baseline

Authority: LocalStack commit `c2cb02372f48cde90b06f0e6ce809a058251fbd7`, audited on 2026-09-02.

| Measure | Current evidence |
|---|---:|
| Requested test forms wired | 7 / 7 (100%) |
| S3 operations routed to emulation | 115 / 115 (100%) |
| Whole-repository statement coverage | 83.8% |
| S3 statement coverage | 89.1% |
| LocalStack S3 test functions explicitly traced | 9 / 463 (1.9%) |
| LocalStack S3 test functions not yet traced | 454 / 463 (98.1%) |

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

## Source-only findings

These fixes are verified against pinned LocalStack implementation paths but do not increase the test-function numerator:

| LocalStack source | Mirror evidence |
|---|---|
| `validate_failed_precondition` exact ETag comparison | PR #229 |
| `get_failed_precondition_copy_source` exact ETag comparison | PR #229 |
| `get_failed_upload_part_copy_source_preconditions` exact ETag comparison | PR #229 |

Operation support is complete at the routing layer. Behavioral parity is not complete or yet quantifiable beyond the explicit 9/463 lower bound; each remaining test function must be mapped, reproduced when divergent, and covered before the parity audit can reach 100%.
