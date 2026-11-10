# DynamoDB — a table switched back from on-demand reads PROVISIONED (2026-11)

## Evidence

- `BillingModeSummary` "contains the details for the read/write capacity
  mode", and its `BillingMode` is `PROVISIONED` or `PAY_PER_REQUEST`
  ([BillingModeSummary](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BillingModeSummary.html)).
  `UpdateTable` takes `BillingMode`: "When switching from pay-per-request to
  provisioned capacity, initial provisioned capacity values must be set"
  ([UpdateTable](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html)).
  The wording is quoted as remembered and is a reading until compared with
  the pages.
- Before, the summary was written only for `PAY_PER_REQUEST`. A table
  switched to on-demand and back kept `BillingModeSummary.BillingMode`
  `PAY_PER_REQUEST` beside its provisioned throughput, and the next update
  that named no mode read the mode from that summary and set the throughput
  to 0 again.

## Behavioral corrections

| Case | Before | After |
|---|---|---|
| `UpdateTable BillingMode=PROVISIONED` with throughput, on a table that is on-demand, then `DescribeTable` | `BillingModeSummary.BillingMode` `PAY_PER_REQUEST`, the throughput given | `PROVISIONED`, the throughput given |
| A later `UpdateTable` of that table's throughput alone | The throughput set to 0, the table read as on-demand | The throughput given |
| A table that was never on-demand, before and after `UpdateTable BillingMode=PROVISIONED` | No `BillingModeSummary` | The same |

## Readings

- That a table never switched to on-demand has no `BillingModeSummary`, and
  one switched back keeps it with `PROVISIONED`, is how the service's
  `DescribeTable` answers are remembered; the reference states neither.
  `LastUpdateToPayPerRequestDateTime` is not answered.
- Left as it is: `UpdateTable TableClass=STANDARD` on a table that was never
  given a class writes `TableClassSummary` `STANDARD` where there was none.
  The reference says of `TableClassSummary` only "Contains details of the
  table class"; whether the service answers one for a table never given a
  class, or refuses an update to the class it has, is not established here.
  A CloudFormation rollback that takes a `TableClass` away sends that
  update, so such a table reads `STANDARD` afterwards, which is its class.
