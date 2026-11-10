# Kinesis — a stream has the shards and the mode it was created with (2026-11)

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
- `CreateStream` takes `StreamModeDetails`, "Indicates the capacity mode of
  the data stream. Currently, in Kinesis Data Streams, you can choose between
  an on-demand capacity mode and a provisioned capacity mode", the summary
  answers it, and `DescribeLimits` answers `OnDemandStreamCount`, "Indicates
  the number of data streams with the on-demand capacity mode", beside
  `ShardLimit`, "The maximum number of shards".
- `UpdateShardCount` lists `ResourceNotFoundException` and
  `LimitExceededException` among its errors, `CreateStream`
  `LimitExceededException`: "The requested resource exceeds the maximum number
  allowed, or the number of concurrent stream requests exceeds the maximum
  number allowed."
- The quotations of `DescribeLimits`, `UpdateShardCount`, `UpdateStreamMode`,
  `StreamModeDetails` and of the Developer Guide's
  [Quotas and limits](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
  were read from the pages in 2026-11. The others are from the API reference
  as remembered: the vendored model file is not in a checkout, so each of
  those is a reading until it is compared with the page.
- The shard quota is the provisioned streams'. The quotas table gives, for
  "Number of shards", under on-demand mode "There's no upper limit. Number of
  shards depends on the amount of data ingested and the level of throughput
  you require. Kinesis Data Streams automatically scales the number of
  shards", and only under provisioned mode "The default shard quota is ...
  shards per AWS account". An on-demand stream has the other quota: "By
  default, you can create up to 50 data streams with the on-demand capacity
  mode."
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
| `CreateStream` with `StreamModeDetails.StreamMode` `ON_DEMAND` and no `ShardCount` | A provisioned stream of one shard; the mode dropped | An on-demand stream of four open shards |
| `DescribeStreamSummary` | No `StreamModeDetails` | The stream's mode: the one it was created with or `UpdateStreamMode` set, `PROVISIONED` when none was given |
| `DescribeLimits` with on-demand streams | `OnDemandStreamCount` 0 | The number of the caller's streams whose mode is `ON_DEMAND` |
| `UpdateShardCount` on a stream that does not exist | 200, and a stream record of that count | `ResourceNotFoundException` |
| `CreateStream` or `UpdateShardCount` of a provisioned stream that would take the caller's provisioned shards past `ShardLimit` (500) | Accepted | `LimitExceededException` |
| `DescribeLimits.OpenShardCount` with on-demand streams | 1 | The provisioned streams' shards; an on-demand stream's are not counted |
| A 51st on-demand stream (`OnDemandStreamCountLimit` 50) | Accepted | `LimitExceededException` |
| `UpdateShardCount` on an on-demand stream | The count set | `ValidationException` |
| `UpdateStreamMode` on a stream that does not exist | 200, and a stream record | `ResourceNotFoundException` |
| `UpdateStreamMode` to `ON_DEMAND` with 50 other on-demand streams, or to `PROVISIONED` when the stream's shards would take the provisioned shards past `ShardLimit` | Accepted | `LimitExceededException` |

## Readings

- A provisioned stream created with no `ShardCount`, or one that is not
  positive, has one shard; the service requires a count there. An on-demand
  stream has four: "By default, new data streams created with the on-demand
  capacity mode have 4 MB/s of write and 8 MB/s of read throughput", and
  "Each shard can support up to 1 MB/sec ... write throughput" (Quotas and
  limits); that this is four shards is the arithmetic, not a sentence of the
  guide. A `ShardCount` given with `ON_DEMAND` is kept as the count.
- The shard limit holds a provisioned stream over the shards of the other
  provisioned streams, less those of the name a create replaces. An
  on-demand stream's shards are neither held to it nor counted in it.
  `DescribeLimits.OpenShardCount` is "The number of open shards", of an
  operation that "Describes the shard limits and usage for the account":
  that it is the count held against `ShardLimit`, the provisioned streams',
  and leaves on-demand shards out, is a reading. The messages of both
  `LimitExceededException`s are this emulator's, and so is the limit of 500
  (the guide's defaults are 1,000, 6,000 or 20,000 by Region).
- `UpdateShardCount` "is only supported for the data streams with the
  provisioned capacity mode" (API reference). Which of its errors answers an
  on-demand stream is not stated: `ValidationException` ("The request failed
  data validation for this data stream") and the message are a reading.
  `SplitShard` and `MergeShards`, "Only applicable for provisioned" (Quotas
  and limits), still move an on-demand stream's count.
- `SplitShard`, `MergeShards` and the retention operations still make a
  record for a stream that does not exist, as the recorded trace has them.
- A CloudFormation `AWS::Kinesis::Stream` does not pass its
  `StreamModeDetails` on: its streams are provisioned.
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
bundle replaced. Three steps are re-cut, each with its reason on the step:
`DescribeStreamSummary` (step 7, `StreamModeDetails` added),
`UpdateShardCount` (step 35, `CurrentShardCount` 2 to 1) and `DescribeLimits`
(step 40, `OpenShardCount` 1 to 4). All are still compared in full.
