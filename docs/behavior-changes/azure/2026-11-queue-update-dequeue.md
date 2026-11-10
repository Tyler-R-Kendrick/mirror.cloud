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
- **Get Messages refuses `visibilitytimeout=0`.** Get Messages documents that
  "a specified value must be larger than or equal to 1 second, and it can't be
  greater than 7 days", and that an out-of-range value is 400
  `OutOfRangeQueryParameterValue`. Before, `0` was accepted and the message was
  dequeued (count and receipt moved) while staying visible. Put Message and
  Update Message still take `0`, as each documents.
- **Update Message with no body keeps the text.** The REST reference describes
  the operation as one that "updates the visibility timeout of a message. You
  can also use this operation to update the contents", and the SDKs make the
  text optional: the JavaScript client documents "if this parameter is
  undefined, then the content of the message won't be updated", and .NET's
  `messageText` is "Optional". A worker renewing its hold on a message sends a
  new timeout and no body. Before, such an update emptied the message. An
  update that sends a `MessageText`, empty or not, still replaces the text.
- **Update Message refuses a timeout past the message's expiry.** Update
  Message documents that "the visibility timeout of a message can't be set to
  a value later than the expiry time". The reference names no error for it,
  and the Queue Storage error-code table has none; the bundle answers what the
  reference gives a visibility timeout it will not take, 400
  `OutOfRangeQueryParameterValue`. Before, the update was accepted and the
  message stayed hidden until it expired. Put Message documents the same rule
  and still accepts such a timeout here.
