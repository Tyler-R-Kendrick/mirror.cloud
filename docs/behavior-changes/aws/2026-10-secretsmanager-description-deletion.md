# Secrets Manager: description and scheduled deletion (2026-10)

The oracle is AWS's documentation as it ships in the vendored model
(`secrets-manager-2017-10-17.json`, sha256 `2738bd8d…`, pinned in
`specs/mirror.lock`).

- **CreateSecret creates; the writes need a secret.** `CreateSecret` of a name
  that exists is `ResourceExistsException` ("A resource with the ID you
  requested already exists"). `PutSecretValue`, `UpdateSecret` and
  `RotateSecret` of a name never created are `ResourceNotFoundException`
  ("Secrets Manager can't find the resource that you asked for"), which each
  documents. Before, the four were one upsert: a second `CreateSecret` wrote a
  new version, and the others created the secret. CloudFormation's
  `UpdateStack` relied on that and now updates its secret through
  `UpdateSecret`.
- **A secret without a value has no version.** `CreateSecret` makes a version
  only "If you include SecretString or SecretBinary"; without either it
  answers no `VersionId`, `GetSecretValue` is `ResourceNotFoundException`, and
  `BatchGetSecretValue` answers a not-found `Errors` row. Before, it stored an
  empty version holding `""`. `PutSecretValue` with neither value ("You must
  include SecretBinary or SecretString") is `InvalidParameterException`;
  before, it stored `""`.
- **A string and a binary together are refused.** `CreateSecret`,
  `PutSecretValue` and `UpdateSecret` each say "Either SecretString or
  SecretBinary must have a value, but not both" (in `PutSecretValue`'s words,
  "You must include SecretBinary or SecretString, but not both"). Both at once
  is `InvalidParameterException` now. The model names no error for the
  combination, so the code is this note's reading: of the errors the three
  operations list, it is the one about the request ("The parameter name or
  value is invalid") rather than the secret's state. Before, the string was
  stored and the binary dropped.
- **An empty value is refused.** `SecretStringType` and `SecretBinaryType`
  have a minimum length of 1. `CreateSecret`, `PutSecretValue` and
  `UpdateSecret` refuse an empty `SecretString` or `SecretBinary` with
  `ValidationException`, the engine's answer to a member outside its length
  constraint. The engine's own check does not fire here because the Smithy
  receiver carries no length traits into any generated AWS model, so the
  bundle states this one. Before, `""` was stored as a version.
- **Description.** `CreateSecret` and `UpdateSecret` keep `Description`, and
  `DescribeSecret` answers it. Before, it was accepted and dropped. A write
  that names no description keeps the one already stored.
- **Deprecated versions are not in the stage maps.** A version with no staging
  label "is considered deprecated and subject to deletion. Such versions are
  not included in this list" (`SecretListEntry.SecretVersionsToStages`;
  `DescribeSecretResponse.VersionIdsToStages` is "A list of the versions of
  the secret that have staging labels attached"). `ListSecrets` and
  `DescribeSecret` leave such a version out; they answered it with `[]`.
  `ListSecretVersionIds` is unchanged and still lists it.
- **ListSecrets answers both.** Each entry carries `Description` ("The
  user-provided description of the secret") and, for a secret scheduled for
  deletion listed with `IncludePlannedDeletion`, `DeletedDate` ("Not present on
  active secrets"), each only when it has one. Before, neither was projected.
- **UpdateSecret without a value.** A description-only `UpdateSecret` adds no
  version and answers no `VersionId`. Only an update that carries
  `SecretString` or `SecretBinary` creates a version. Before, every update
  appended a version holding `""` and made it `AWSCURRENT`.
- **A secret scheduled for deletion is described.** `DescribeSecret` answers it
  with `DeletedDate`, where it answered `InvalidRequestException` before.
  `GetSecretValue` still refuses it.
- **`DeletedDate` is when the deletion was requested.** The model documents the
  member two ways: `SecretListEntry.DeletedDate` is "The date and time the
  deletion of the secret occurred", and `DescribeSecretResponse.DeletedDate`
  is "The date the secret is scheduled for deletion". AWS answers the time of
  the `DeleteSecret` request, which is the first reading, so that is what is
  stored. The end of the window is `DeleteSecret`'s `DeletionDate`, "the date
  and time of the delete request plus the number of days in
  RecoveryWindowInDays".
- **DeleteSecret.**
  - The answer is `ARN`, `Name` and `DeletionDate`. `DeletionDate` is the
    request time plus `RecoveryWindowInDays`, 30 when not given; before, it was
    the request time.
  - A second unforced delete is `InvalidRequestException` ("The secret is
    scheduled for deletion"). Before, it was scheduled again.
  - A forced delete of a secret that is already deleted or never existed
    succeeds.
  - A forced delete answers `DeletionDate` as the time of the request: with no
    recovery window, "the date and time of the delete request plus the number
    of days in RecoveryWindowInDays" is the request itself. It answered null.
  - `ForceDeleteWithoutRecovery` together with `RecoveryWindowInDays` is
    `InvalidParameterException`.
  - `RecoveryWindowInDays` must be "from 7 to 30"; any other value is
    `InvalidParameterException`, checked before the date is computed. On
    `main` the window was ignored and the request time answered; the
    overflow a huge window caused existed only between commits on this
    branch.
  - `RecoveryWindowInDays` is a `long` (`RecoveryWindowInDaysType`). A value
    that is not a whole number (`7.5`, `"7"`, `true`) is
    `SerializationException`, what the `awsJson1_1` codec answers a body it
    cannot decode. The engine checks no member types and no bundle in the
    repository refuses a wrongly typed member, so the codec's answer is the
    convention used. Before, `7.5` and `"7"` were read as 7. A whole number
    sent as a JSON float (`7.0`) is still 7.
  - A primary secret replicated to another Region is not deleted, forced or
    not: "You can't delete a primary secret that is replicated to other
    Regions. You must first delete the replicas using
    RemoveRegionsFromReplication, and then delete the primary secret." It is
    `InvalidRequestException`, the error for the secret's current state.
    Before, it was deleted with its replicas.
- **RestoreSecret** clears `DeletedDate` and answers `ARN` and `Name`.
- **A secret scheduled for deletion refuses writes.** Every operation that
  writes to a secret answers `InvalidRequestException` ("Possible causes: The
  secret is scheduled for deletion."), which each of them lists among its
  errors, and leaves the secret as it was: `CreateSecret` on its name,
  `PutSecretValue`, `UpdateSecret`, `RotateSecret`, `CancelRotateSecret`,
  `UpdateSecretVersionStage`, `TagResource`, `UntagResource`,
  `PutResourcePolicy`, `DeleteResourcePolicy`, `ReplicateSecretToRegions`,
  `RemoveRegionsFromReplication` and `StopReplicationToReplica`. Before, the
  first four wrote the value and undeleted the secret, and the rest changed
  its stages, tags, policy or replicas. No write operation leaves the error
  out of its list, so none is left unchanged for that reason. `RestoreSecret`
  is the way back, and `DeleteSecret` has its own rules (below). The reads are
  unchanged: `DescribeSecret` and `ListSecretVersionIds` do not list the
  error, and `GetResourcePolicy`, which does, writes nothing.
  `GetSecretValue` refused it already ("When a secret is scheduled for
  deletion, you cannot retrieve the secret value. You must first cancel the
  deletion with RestoreSecret").
- **Filters choose secrets.** `BatchGetSecretValue` and `ListSecrets` apply
  `Filters` as `Filter.Key` describes them: `name`, `tag-key`, `tag-value` and
  `primary-region` are case-sensitive prefix matches, `description` a prefix
  match that is not case-sensitive, and `all` "Breaks the filter value string
  into words and then searches all attributes for matches. Not
  case-sensitive." A value starting with `!` negates. Every filter must
  match, a filter matches when any of its values does, and with `all` every
  word must start a word of some attribute; the model does not say how
  values combine, so that is this note's reading. Before, both operations
  ignored `Filters`.
  - `BatchGetSecretValue` refuses `Filters` with `SecretIdList` ("You must
    include Filters or SecretIdList, but not both") and a request with
    neither, with `InvalidParameterException`. The filter form pages with
    `MaxResults` and `NextToken` as `ListSecrets` does, leaves out a secret
    scheduled for deletion as `ListSecrets` does, and answers a secret with no
    value as a not-found `Errors` row. The `SecretIdList` form is unchanged.
  - A listing page that keeps nothing while more secrets follow now moves
    `NextToken` past what it read; before, `ListSecrets` with a scheduled
    secret between two listed ones lost the second (an engine fix).
- **BatchGetSecretValue leaves out a secret scheduled for deletion.** Its value
  is not answered; it is an `Errors` row, an `APIErrorType` of `SecretId`,
  `ErrorCode` `InvalidRequestException` and `Message`. Before, its value was
  answered in `SecretValues`.
- **A member with no value is left out.** `DescribeSecret` "only returns
  fields that have a value in the response", so `Description`, `DeletedDate`,
  `Tags`, `ReplicationStatus` and the rotation members are absent when unset,
  not null. So are `UpdateSecret`'s `VersionId` when it made no version and
  `DeleteSecret`'s `ARN` for a secret that never existed. `Tags` is left out of
  a `ListSecrets` entry and of `DescribeSecret` when the secret has none,
  including after its last tag is removed; the listing wrote `"Tags": null`
  and both wrote `[]`.
- **Timestamps are epoch seconds.** `DeletionDate` and `DeletedDate` are
  timestamps (`DeletionDateType`, `DeletedDateType`) on an `awsJson1_1`
  service with no `timestampFormat` trait, so they are written as epoch
  seconds, a JSON number. They were date strings, which the Go SDK v2 refuses:
  it decodes a timestamp only from a JSON number, so `DescribeSecret` of a
  secret scheduled for deletion failed in every Go SDK v2 client, Terraform's
  included. `main` wrote `DeletionDate` as a string too. They carry
  milliseconds where the clock has them (`1.5`, `2592001.5`), as the model's
  examples do ("CreatedDate": 1523477145.713); they were whole seconds.

Equivalence trace `aws.secretsmanager`, steps counted from 0. Step 48, the
pack's delete of `primary`, is refused now, because `primary` has had a
replica in us-west-2 since step 35. Steps 49 to 59 are added. Step 49 removes
the replica and step 50 schedules `primary` for deletion. Each of steps 51 to
59 is then made against the scheduled secret: a `PutSecretValue`, a
`BatchGetSecretValue`, and one each of `UpdateSecretVersionStage`,
`PutResourcePolicy`, `DeleteResourcePolicy`, `ReplicateSecretToRegions`,
`RemoveRegionsFromReplication`, `StopReplicationToReplica` and
`CancelRotateSecret`. Steps 37, 45, 46, 47, 48, 61, 62, 63, 64 and 67 are
re-cut, and every step whose reason names another step was re-cut again to
name it where it now is. Each carries the sentence above that it now follows.
The first commit's re-cuts at 52, 53 and 56 are now at 63, 64 and 67.
