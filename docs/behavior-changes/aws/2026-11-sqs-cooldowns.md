# SQS: the 60-second windows run their whole length (2026-11)

The oracle is the SQS API reference (`sqs-2012-11-05`).

- **CreateQueue after DeleteQueue.** `DeleteQueue` documents "you must wait at
  least 60 seconds before creating a queue with the same name". A
  `CreateQueue` of that name is `QueueDeletedRecently` until 60 seconds after
  the delete.
- **PurgeQueue.** `PurgeQueueInProgress` is documented as "the specified queue
  previously received a PurgeQueue request within the last 60 seconds". A
  second `PurgeQueue` is refused until 60 seconds after the first.

Before, both windows were measured in whole clock seconds, so the part of the
second already spent when the first request arrived was taken off them: a
delete at 12:00:00.990 allowed a create at 12:01:00.000, 59.01 seconds later.
Each window was short by up to a second.
