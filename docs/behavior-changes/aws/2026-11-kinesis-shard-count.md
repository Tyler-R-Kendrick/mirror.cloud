# Kinesis — a stream has the shards it was created with (2026-11)

## Evidence

- `CreateStream` takes `ShardCount`, "The number of shards that the stream
  will use"
  ([CreateStream](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html)),
  and `DescribeStreamSummary` answers `OpenShardCount`, "The number of open
  shards in the stream"
  ([StreamDescriptionSummary](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescriptionSummary.html)).
- `UpdateShardCount` answers both counts: `CurrentShardCount`, "The current
  number of shards", and `TargetShardCount`, "The updated number of shards"
  ([UpdateShardCount](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_UpdateShardCount.html)).
- `DescribeLimits` answers `OpenShardCount`, "The number of open shards", for
  the account in the Region
  ([DescribeLimits](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeLimits.html)).
- The quoted wording is from the API reference as remembered: the vendored
  model file is not in a checkout, so each quotation is a reading until it is
  compared with the page.
- Before, `CreateStream` accepted `ShardCount` and dropped it. A
  CloudFormation stream whose template gave three shards was made with one,
  and a rollback that put the template's count back could only do it with
  `UpdateShardCount`.

## Behavioral corrections

| Case | Before | After |
|---|---|---|
| `CreateStream` with `ShardCount` 3, then `DescribeStreamSummary` | `OpenShardCount` 1 | 3 |
| `DescribeStreamSummary` after `UpdateShardCount`, `SplitShard` or `MergeShards` | 1, whatever the count on the record | The count on the record |
| `UpdateShardCount` from 3 to 5 | `CurrentShardCount` 5, `TargetShardCount` 5 | `CurrentShardCount` 3, `TargetShardCount` 5 |
| `DescribeLimits` with two streams of 3 and 1 shards | `OpenShardCount` 1 | 4, the sum over the caller's streams in the Region (0 with no stream) |
| `CreateStream` over an existing name | The record replaced, the count gone | The record replaced, with the count this request gives |

## Readings

- A stream created with no `ShardCount`, or one that is not positive, has one
  shard. The service requires a count for a provisioned stream and gives an
  on-demand stream its own initial shards; neither is modelled, and
  `StreamModeDetails` is still dropped on create.
- `DescribeStream` and `ListShards` still list exactly one shard,
  `shardId-000000000000`, with the full hash-key range, whatever the count.
  Shards are not records here: listing a stream's real shards needs one
  record per shard with its hash-key range (a 128-bit split the bundle's
  expressions cannot compute), parent and adjacent shards for splits and
  merges, and a record plane that routes a partition key to a shard and
  hands out one iterator per shard. `PutRecord` writes to that one shard.
- `UpdateShardCount` applies at once: the stream is never `UPDATING`, and
  none of the service's limits on how far or how often a count may move is
  applied.

## Recorded trace

`internal/equivalence/traces/aws.kinesis.json` was recorded from the pack the
bundle replaced. Two steps are re-cut, each with its reason on the step:
`UpdateShardCount` (step 35, `CurrentShardCount` 2 to 1) and `DescribeLimits`
(step 40, `OpenShardCount` 1 to 4). Both are still compared in full.
