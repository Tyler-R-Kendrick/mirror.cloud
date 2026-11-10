# Queue Storage: Update Message, DequeueCount and Get Messages' range (2026-11)

The oracle is the Queue Storage REST API reference (Put Message, Get Messages,
Update Message).

- **Update Message stores the update whenever the pop receipt is good.**
  Update Message documents that a pop receipt remains valid until the message
  expires, is deleted, is updated, or "the invisibility time has elapsed and
  the message has been dequeued by a Get Messages request"; Put Message
  documents that its `PopReceipt` "may be used together with" the message id
  "to delete or update a message". So a message that is visible, because its
  timeout ran out, because it was last updated with `visibilitytimeout=0`, or
  because it was never dequeued, is updated on its current receipt: the text
  and the new receipt are stored and the message is hidden for the timeout
  given. Before, such an update answered 204 and a new `x-ms-popreceipt` and
  stored neither: the text was unchanged and the receipt it returned was
  refused.
- **`DequeueCount` is reported once a message has been dequeued.** Get
  Messages documents that "the `DequeueCount` element has a value of 1 the
  first time the message is dequeued. This value is incremented each time the
  message is subsequently dequeued." Before, Get Messages and Peek Messages
  rendered `<DequeueCount></DequeueCount>`, empty, for any message that had
  been dequeued; only a never-dequeued message carried a number (`0`).
