# CloudFormation — no stack is left in progress, and what a failed update leaves (2026-11)

## Evidence

- The API reference gives each stack operation its states: a failed create
  rolls back and may end `ROLLBACK_FAILED`, a failed delete ends
  `DELETE_FAILED` and "Resources that failed to delete" stay for the next
  `DeleteStack`, and a stack `*_IN_PROGRESS` takes no other change
  ([DeleteStack](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_DeleteStack.html),
  [Stack statuses](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/view-stack-events.html#cfn-console-view-stack-data-resources-status-codes)).
- A failure of the service's own is `InternalFailure`, HTTP 500: "The request
  processing has failed because of an unknown error, exception or failure"
  ([Common Errors](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/CommonErrors.html)).
- Before, an engine panic, a store write that failed, or a process that
  ended mid-operation left the record `CREATE_IN_PROGRESS`,
  `UPDATE_IN_PROGRESS` or `DELETE_IN_PROGRESS` for good: no operation took
  the stack again, `DeleteStack` included.

## Behavioral corrections

| Case | Before | After |
|---|---|---|
| A record `*_IN_PROGRESS` whose operation is not running (it ended without settling the record, or its process ended) | Stuck: every later operation refused or ignored it | The next `CreateStack`, `UpdateStack`, `DeleteStack`, `CreateChangeSet` or `ExecuteChangeSet` on the stack settles it first, as that operation's failure, and goes on from there. A create is `ROLLBACK_FAILED`; a delete is `DELETE_FAILED`; an update is the status it found, with what it had yet to delete left on the stack. `StackStatusReason` says why. `REVIEW_IN_PROGRESS` is no operation's and is left |
| A read (`DescribeStacks`, `ListStacks`, the resource reads) of such a record, before any operation meets it | The in-progress status | The same: a read settles nothing |
| A stack a running operation holds | Refused, or answered, as each state is | The same: a running operation is never taken over |
| A panic in the engine, or in a service it calls, during an operation | Escaped the request, or left the stack in progress | `InternalFailure` (500) with the generic message; the operation's ordinary cleanup follows, and the stack's reason is the error answered |
| A bucket whose configuration failed after `CreateBucket` | In no stack's record: the rollback left it | Recorded as soon as it is made, so the rollback or the failed update deletes it |
| What a failed update made and could not delete | Dropped from the record | Kept on the stack, apart from its resources (so a logical id is listed once), `DELETE_FAILED` with the error. It counts as the stack's: another stack cannot take it, the stack's next update takes it back when its template declares it, a later successful update deletes it otherwise, and `DeleteStack` deletes it. A `DeleteStack` that cannot delete it fails, `DELETE_FAILED` |
| A failed update of a secret, a rule or an SSM parameter it changed in place | Left changed | Put back from the previous template; one it cannot put back is named in the reason, "The following resource(s) failed to update: [A]." |
| A named resource whose owner's lookup fails, other than with its not-found error | Read as a free name, and created | The operation fails with the lookup's error |
| An `UpdateStack` whose stack changes between its read and its reservation | Applied its previous values from the stale read | Read again, three times at most, then `Throttling`, "Rate exceeded" |

## Readings

- The reason of a stack taken over, "The operation on the stack ended before
  it settled the stack.", and of a resource a cleanup did not reach, "not
  deleted: the operation ended first", are this emulator's words.
- An operation is known to be running by an id on its record that this
  process holds in memory. A record another process left is therefore taken
  over at once: the emulator is one process, and a second one sharing its
  store would have its running operations taken over.
- A create whose process ended lists none of what it made: the record is
  written when the create ends. Within one process, what a failed operation
  could not write is kept and written by the operation that takes over.
