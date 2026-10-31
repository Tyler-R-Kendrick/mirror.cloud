# SQS and SNS — query-protocol entry names and the answers that depend on them (2026-10)

## Evidence

- The AWS query decoder read every map entry as `Name.entry.N.key` /
  `.value` and every non-flattened list as `Name.member.N`, ignoring the
  xmlName the model gives a map's key and value or a list's member. SQS's
  queue attributes, message attributes and tags, SNS's message attributes, and
  81 list shapes in awsQuery inputs (RDS, Redshift, ElastiCache, DocumentDB,
  Neptune, Auto Scaling) are named that way, so what an SDK sends for them was
  read as absent.
- AWS docs: the SQS attribute digest
  ([MD5 message digest calculation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-attributes-md5-message-digest-calculation)),
  `SendMessageBatchResultEntry`, the SNS notification body
  ([HTTP/HTTPS notification JSON](https://docs.aws.amazon.com/sns/latest/dg/http-notification-json.html)),
  the Lambda SNS event, and `PublishBatch`'s `PublishBatchRequestEntries`,
  `Successful` and `Failed`.

## Behavioral corrections

| Operation | Before | After |
|---|---|---|
| SQS CreateQueue / SetQueueAttributes (query) | `Attribute.N.Name/Value` dropped: `.fifo` refused, `VisibilityTimeout` ignored, SetQueueAttributes `MissingParameter` | Read |
| SQS SendMessage (query) | `MessageAttribute.N.*` dropped silently; no `MD5OfMessageAttributes` | Stored and delivered; `MD5OfMessageAttributes` answered |
| SQS SendMessage (query), attribute with no DataType | Attribute dropped, message accepted | 400 `InvalidParameterValue` |
| SQS SendMessageBatch | Entries answered MessageId, MD5OfMessageBody, SequenceNumber | Also `MD5OfMessageAttributes` and `MD5OfMessageSystemAttributes`, when the entry has attributes |
| SNS notification (SQS without raw delivery, HTTP/S) | `MessageAttributes` as `{DataType, StringValue}`, or on `main` a mixture with `Name`, `Type`, `Value` | Exactly `{Type, Value}`; Binary as base64 |
| SNS Lambda event | Same as the notification | `{Type, Value}`; every type but Binary is `String` |
| SNS PublishBatch (query) | `PublishBatchRequestEntries` unread: every request `EmptyBatchRequest` | Entries published, each with its own notification |
| SNS PublishBatch, an entry refused after a valid one | Valid entries delivered, then a 400 | Nothing delivered, then the 400 |
| SNS PublishBatch answer | `Successful` only | `Successful` and an empty `Failed` |
| RDS and other awsQuery lists named by their member | `SubnetIds.SubnetIdentifier.N`, `Parameters.Parameter.N`, `Tags.Tag.N` read as absent | Read; `.member.N` still accepted |

## Equivalence

No recorded trace changes: the traces send no named entries, and the
decoder still reads `.member.N` and `key`/`value` where a client sends them.
