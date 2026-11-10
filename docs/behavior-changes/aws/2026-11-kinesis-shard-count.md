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
| `CreateStream` or `UpdateStreamMode` with a `StreamMode` that is not `PROVISIONED` or `ON_DEMAND` | Recorded as given | `ValidationException`, "1 validation error detected: Value 'X' at 'streamModeDetails.streamMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, ON_DEMAND]" |
| `SplitShard` or `MergeShards` on an on-demand stream | The count moved | `ValidationException` |
| `SplitShard` or `MergeShards` on a stream that does not exist | 200, and a stream record | `ResourceNotFoundException`, "Stream <name> under account <id> not found." |
| `IncreaseStreamRetentionPeriod` or `DecreaseStreamRetentionPeriod` on a stream that does not exist | 200, and a stream record that `DescribeStream` then answered | `ResourceNotFoundException`, with the message; no record |
| `DescribeStream`, `DescribeStreamSummary`, `PutRecord` or `DescribeStreamConsumer` of what does not exist | `ResourceNotFoundException` with no message | The same, with the message |
| A third change of a stream's mode within 24 hours | Accepted | `LimitExceededException` |
| `CreateStream` with a `ShardCount`, or `UpdateShardCount` with a `TargetShardCount`, of 0 or less | The default made (one shard, or four on-demand); 200 and nothing changed | `ValidationException`, "1 validation error detected: Value '0' at 'shardCount' failed to satisfy constraint: Member must have value greater than or equal to 1" (`targetShardCount` for the other) |
| Any operation with a member that should be a string given an object or a list, or one that should be an integer given anything but a whole number | `InternalFailure` (500) wherever the member was read | `ValidationException`, "<Member> must be a string" or "<Member> must be an integer", before any other rule |
| Any operation with a member given as JSON `null` | `InternalFailure` (500) wherever a string or integer member was read; otherwise read as present | Read as absent: the answer is the one without the member, and a required member is missing ("<Member> is required") |
| An operation on a stream that does not exist: `DeleteStream`, `ListShards`, `StartStreamEncryption`, `StopStreamEncryption`, `EnableEnhancedMonitoring`, `DisableEnhancedMonitoring`, `UpdateStreamWarmThroughput`, `UpdateMaxRecordSize`, `AddTagsToStream`, `RemoveTagsFromStream`, `ListTagsForStream`, `RegisterStreamConsumer`, `ListStreamConsumers`, `GetShardIterator` | 200, and for most a stream or tag record | `ResourceNotFoundException`, "Stream <name> under account <id> not found."; nothing is written |
| `PutRecords` to a stream that does not exist | 200, `FailedRecordCount` 0, and `InternalFailure` for each record | `ResourceNotFoundException` |
| `GetRecords` through the iterator of a stream since deleted | 200, with the records of a stream made again under the name | `ResourceNotFoundException` |
| `TagResource`, `UntagResource`, `ListTagsForResource`, `PutResourcePolicy`, `GetResourcePolicy` or `DeleteResourcePolicy` with the ARN of a stream, or of a consumer, that does not exist | 200, and a tag or policy record (an empty policy for `GetResourcePolicy`) | `ResourceNotFoundException`, naming the stream or the consumer |
| `DeregisterStreamConsumer` or `SubscribeToShard` of a consumer that does not exist | 200 | `ResourceNotFoundException`, "Consumer <ARN> under account <id> not found." |
| `DescribeStream`, `DescribeStreamSummary`, `DeleteStream`, `PutRecord`, `PutRecords` or `GetShardIterator` of a stream that exists, addressed by `StreamARN` alone | `ResourceNotFoundException`, with no name in its message | The stream, as by its name |
| `DescribeStreamConsumer` or `DeregisterStreamConsumer` by `StreamARN` and `ConsumerName` | Not found, with no name in its message | The consumer, as by its ARN |
| An operation that takes a `StreamName` or a `StreamARN`, given neither | `ResourceNotFoundException`, "Stream  under account ..." | `InvalidArgumentException`, "Either StreamName or StreamARN must be specified." (for a consumer: "Either ConsumerARN, or StreamARN and ConsumerName, must be specified.") |
| An address that names nothing: an empty string, whitespace, or a `StreamARN` that ends at `stream/` or in a slash | "Stream  under account ... not found." with no name; `RegisterStreamConsumer` made a consumer `arn:...:stream//consumer/<name>` | As an address not given: "<Member> is required" where the member is required, `InvalidArgumentException` where it is one of several; nothing is written |
| `GetRecords` with an iterator that names no stream | `InvalidArgumentException`, "Either StreamName or StreamARN must be specified." | `InvalidArgumentException`, "Invalid ShardIterator.", as for any value that is no iterator |
| `CreateStream` with a `StreamName` outside `[a-zA-Z0-9_.-]+`, or longer than 128 | Accepted | `ValidationException` |
| `PutRecord`, `PutRecords` or `GetShardIterator` with a `StreamName` that holds a slash | The name cut at its last slash, so `a/b` was the stream `b` | The name whole: only an ARN is cut |
| A pipe made before its Kinesis stream, with `StartingPosition` `LATEST` | The first record put to the stream once it existed was never delivered | The pipe reads the stream from its beginning once it exists |
| `UpdateStreamMode` with no `StreamModeDetails`, or a null one | `ValidationException`, "StreamModeDetails is required" | The same (it was `InternalFailure`, 500, between the two changes of this note) |
| `CreateStream` or `UpdateStreamMode` with a `StreamModeDetails` that is `{}`, holds a null `StreamMode`, or is no structure | Taken as provisioned, or `InternalFailure` | `ValidationException` |
| `UpdateStreamMode` on a stream that does not exist | 200, and a stream record | `ResourceNotFoundException` |
| `UpdateStreamMode` to `ON_DEMAND` with 50 other on-demand streams, or to `PROVISIONED` when the stream's shards would take the provisioned shards past `ShardLimit` | Accepted | `LimitExceededException` |

## Readings

- A provisioned stream created with no `ShardCount` has one shard; the
  service requires a count there. (A count below one is refused; see the
  table.) An on-demand
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
- Every `ResourceNotFoundException` carries a message. A stream's is "Stream
  <name> under account <id> not found.", the service's as its clients
  report it (the reference says only that the message "provides information
  about the error"; a reading). A consumer's, from `DescribeStreamConsumer`,
  is worded the same way with its ARN, which is this emulator's choice.
- `StreamMode` has "Valid Values: `PROVISIONED | ON_DEMAND`" (API reference,
  StreamModeDetails). The message for any other is the form the service's
  request validation gives an enum; that it is these words, and the order of
  the values in them, is a reading.
- `UpdateShardCount` "is only supported for the data streams with the
  provisioned capacity mode" (API reference). Which of its errors answers an
  on-demand stream is not stated: `ValidationException` ("The request failed
  data validation for this data stream") and the message are a reading.
  `SplitShard` and `MergeShards` say the same of themselves ("This API is
  only supported for the data streams with the provisioned capacity mode"),
  and are answered the same way, by the same reading.
- A CloudFormation `AWS::Kinesis::Stream` has the mode its template gives:
  `StreamModeDetails` goes to `CreateStream`, and its "Update requires: No
  interruption" (Template Reference) is an `UpdateStreamMode` in place. A
  rollback, and a continued one, put the previous mode back, and a template
  that stops giving a mode leaves a provisioned stream, cleared only when
  the previous template gave one.
- A CloudFormation stream's shard count is sent, after the mode, when its
  template's `ShardCount` differs from what the previous template gave, or
  when the update takes the stream from on-demand by its template to not
  on-demand and the template gives a count; never to a stream whose
  template makes it on-demand. The second case is there because a count
  given or changed while the template made the stream on-demand was never
  sent: `{}`, then `ON_DEMAND` with 7, then `PROVISIONED` with 7 ended
  with one shard, and `ON_DEMAND` with 7, then with 9, then `PROVISIONED`
  with 9 ended with seven. A restore does the same, so a rollback from
  on-demand to a provisioned template puts that template's count back. A
  continued rollback does the same, exactly: the record of a stream left
  `UPDATE_FAILED` keeps the mode the failed update gave it (beside the
  names of what changed; a capacity mode is no secret), so the count is sent when that was
  `ON_DEMAND` and not when the update only spelled out `PROVISIONED`. A
  record written before the mode was kept continues as before this rule:
  its count is sent only when the count itself changed. A template that
  gives no count is sent none on leaving on-demand, and keeps the shards
  it has, when the on-demand template gave no count either; when that one
  did, the count has been taken out, and the stream is given one shard, as
  a count taken out of any template is (`ON_DEMAND` with 7, then with 9,
  then `{}` ends `PROVISIONED` with one). A change of mode
  in place leaves the stream's shards as they are, as Kinesis itself does.
  Two consequences are readings of what the service's handler would do,
  pinned as they are: a stream made on-demand in place keeps the count it
  had (a provisioned stream of one shard given `ON_DEMAND` and a
  `ShardCount` of 7 is on-demand with one), where a stream created from the
  same template has the template's count or four; and a rollback to
  on-demand, after an update that had made the stream provisioned with five
  shards, leaves five. An on-demand stream's count is the service's, not
  the template's: a `ShardCount` added to, changed in or taken out of such
  a template is not sent.
- A stream whose mode was set to on-demand outside a template that gives a
  count and no mode is updated as any other; before, every update of it
  failed and the stack was left `UPDATE_ROLLBACK_FAILED`. An update that
  changes that template's count fails with Kinesis's refusal and rolls back
  to `UPDATE_ROLLBACK_COMPLETE`: the restore's count is refused as the
  update's was, and since the update's never applied there is nothing to
  put back, so that one refusal, of the count call alone, does not stop a
  rollback or `ContinueUpdateRollback`. It is recognised whole, by its code
  and its full sentence, and only on a stream: an error that merely
  contains those words, as `ResourceNotFoundException` does for a stream so
  named, or the enum refusal for a stored mode so spelled, is a failed
  restore like any other. Any other restore that fails is a failed
  rollback. The same reading holds on a continued rollback of a count that
  did apply: `ShardCount` 2 updated to 5, the restore failing for another
  reason, the stream then made on-demand outside the stack, ends
  `UPDATE_ROLLBACK_COMPLETE` with the stream on-demand and five shards.
- A request's `StreamModeDetails` that is absent or null is not given:
  `CreateStream` makes a provisioned stream and `UpdateStreamMode` answers
  "StreamModeDetails is required", as for any required member, before
  anything else. One that is given must be a structure with a `StreamMode`
  ("Required: Yes") of the two values. The messages for a `StreamMode` that
  is missing or null ("Member must not be null") and for a value that is no
  structure are this emulator's, in the service's form.
- A stream record stored with a `StreamModeDetails` that is not such a
  structure (an earlier `UpdateStreamMode` stored what it was given) is a
  provisioned stream to every operation.
- `DescribeStream` and `ListShards` still list exactly one shard,
  `shardId-000000000000`, with the full hash-key range, whatever the count.
  This is a known limitation, left as it is. Shards are not records here:
  listing a stream's real shards needs one record per shard with its
  hash-key range (a 128-bit split the bundle's expressions cannot compute),
  parent and adjacent shards for splits and merges, and a record plane that
  routes a partition key to a shard and hands out one iterator per shard.
  `PutRecord` writes to that one shard, and an iterator does not read the
  shard it was asked for, so listing more shards than the record plane has
  would be worse than listing one: a consumer that reads every listed
  shard would be given every record once per shard.
- "For each data stream in your AWS account, you can switch between the
  on-demand and provisioned capacity modes twice within 24 hours" (Quotas
  and limits). A third change within 24 hours of the first of two is
  refused. The guide names no error and the `UpdateStreamMode` page does
  not mention the limit: `LimitExceededException` (which the operation
  lists, "The requested resource exceeds the maximum number allowed"), its
  message, that the 24 hours are a moving window, and that a call for the
  mode the stream already has is no switch, are readings. A stream created
  again over its name starts afresh.
- CloudFormation does nothing special about that limit. An update, a
  rollback or a continued rollback that needs a change of mode Kinesis
  refuses fails as any refused call does: a rollback is left
  `UPDATE_ROLLBACK_FAILED` with Kinesis's reason, and
  `ContinueUpdateRollback` succeeds once the window has passed.
- "Valid Range: Minimum value of 1" is the reference's for `CreateStream`'s
  `ShardCount` and `UpdateShardCount`'s `TargetShardCount`, and
  `ValidationException` is the one for "a parameter value that is outside
  its valid range" (`CreateStream`, Errors). The message is a reading, in
  the form the service uses for a constraint, and so is that the count is
  judged before the stream is looked for. A CloudFormation stream whose template gives 0 or -1 fails
  its create, or its update, which rolls back to the count it had.
- A member that cannot be what the model declares is refused by the engine
  for this bundle (`malformed_input_error`, a bundle option; no other
  service is changed): an object or a list for a string, and for an
  integer also a Boolean, a fraction, text that is not a whole number, or a
  number no 64-bit integer holds. The service itself refuses such a request
  when it reads it; `ValidationException` and the two messages are this
  emulator's, a reading, and so are two leniencies: a string member given a
  number or a Boolean is read as its text, and an integer written as text
  ("3") is read as the number. Every operation of the model is swept with
  each such value in each member, and in each spelling the bundle reads
  that the operation's model does not declare (`StreamArn`, `ResourceArn`,
  `ConsumerArn`, and a `StreamName` or `ResourceARN` where there is none),
  with a provisioned and an on-demand stream present: none answers a 5xx.
- A member that is JSON `null` is absent, with the same option. This one
  is not a reading: Smithy's awsJson1.1 compliance test for servers,
  `AwsJson11ServersDontDeserializeNullStructureValues` ("Null structure
  values are dropped", `smithy-aws-protocol-tests`,
  `model/awsJson1_1/null.smithy`, read 2026-11), sends `{"string": null}`
  and expects no member. The null is dropped before the required-member
  check, the check above and the operation's rules, so a required member
  that is null is missing. Only top-level members: a null inside a
  structure or a list is not judged, nor is any nested member's type, apart
  from `StreamModeDetails.StreamMode`, which the bundle reads itself.
- 2^63-1 written as a JSON number is refused as an integer where the same
  digits as text pass: a JSON number is a 64-bit float by the time it is
  judged, and 2^63-1 has become 2^63, which no int64 holds. Left so; no
  count the service takes is near it.
- A stream is found by its name or by its ARN: "When you invoke this API,
  you must use either the StreamARN or the StreamName parameter, or both"
  (API reference, on each operation that takes the two; read on
  `DeleteStream`). A consumer "provide its ARN. Alternatively, you can
  provide the ARN of the data stream and the name you gave the consumer"
  (`DeregisterStreamConsumer`). A request that gives neither is refused
  with `InvalidArgumentException`, "A specified parameter exceeds its
  restrictions, is not supported, or can't be used", which each of these
  operations lists; that it is that error, and both messages, are
  readings.
- Every operation that takes a stream, a resource ARN or a consumer lists
  `ResourceNotFoundException` in the service's model, which is what the
  API reference's Errors sections print (read on `DeleteStream`,
  `DeregisterStreamConsumer` and `GetResourcePolicy`: "The requested
  resource could not be found. The stream might not be specified
  correctly."), so none was left answering 200 for what is not there;
  "If an application attempts to operate on a deleted stream, it receives
  the exception ResourceNotFoundException" (`DeleteStream`). The message
  of a consumer that is not found, and that a resource ARN which is no
  stream's ARN is reported as a stream not found, are readings. A
  CloudFormation stack whose stream is gone still deletes: a delete that
  finds its resource gone is done.
- An address that names nothing is one not given. The reference gives
  each of these members a minimum length of 1 and an ARN a pattern, and
  says no more; that an empty or blank one is answered as a missing one,
  rather than as its own validation error, is a reading. An ARN's account
  and Region are not checked, and a name wins over an ARN that disagrees
  with it, as before.
- An iterator that is not one is `InvalidArgumentException`, "A specified
  parameter exceeds its restrictions, is not supported, or can't be used",
  which `GetRecords` lists; `ExpiredIteratorException` is "The provided
  iterator exceeds the maximum age allowed", which a malformed one does
  not. The message, "Invalid ShardIterator.", is a reading.
- A stream's name: "Length Constraints: Minimum length of 1. Maximum
  length of 128. Pattern: `[a-zA-Z0-9_.-]+`" (`CreateStream`). A name
  outside it is `ValidationException` when the stream is made; the two
  messages are readings in the service's form for a constraint. No test,
  recorded step or other service's fixture in this repository made a
  stream with such a name. The other operations do not judge a name's
  form: a name that no stream has is not found, whole.
- A pipe whose Kinesis stream does not exist: the `CreatePipe` page lists
  `NotFoundException`, "An entity that you specified does not exist", and
  does not say whether a source that does not exist is one. The emulator
  keeps what it did: the pipe is made, and reads the stream from its
  beginning (`TRIM_HORIZON`) once there is one, whatever its
  `StartingPosition`; a pipe whose stream was there when it first asked
  starts where it was told. All of that is a reading. The same holds for
  a DynamoDB stream source, which shares the code.
- `CreateStream` over a name that exists replaces the record, as the pack
  did (the service lists `ResourceInUseException` for `CreateStream`; that
  a name in use is what it answers is a reading); so the stream's
  shards, mode and the window of its mode changes start afresh. Left so.
- The emulator's consumer ARN has no creation timestamp after the name,
  which the service's has; a stream's tags by name and by resource ARN are
  two sets; a deleted stream's tags, policy and consumers stay. Left so.
- `UpdateShardCount` and `UpdateStreamMode` apply at once: the stream is
  never `UPDATING`, and none of the service's limits on how far or how
  often a count may move is applied.

## Recorded trace

`internal/equivalence/traces/aws.kinesis.json` was recorded from the pack the
bundle replaced. Nine steps are re-cut, each with its reason on the step:
`DescribeStreamSummary` (step 7, `StreamModeDetails` added),
`UpdateShardCount` (step 35, `CurrentShardCount` 2 to 1), `DescribeLimits`
(step 40, `OpenShardCount` 1 to 4), and six that succeeded on what was not
there and now answer `ResourceNotFoundException`: `GetResourcePolicy` of a
stream that does not exist (step 23, an empty policy),
`IncreaseStreamRetentionPeriod` and `DescribeStream` of one (steps 47 and
48), `DeleteStream` of it (step 49), the second `DeregisterStreamConsumer`
of a consumer (step 51) and the second `DeleteStream` of a stream (step
53). All are still compared in full.
