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
  member two ways, and the two conflict: `SecretListEntry.DeletedDate` is "The
  date and time the deletion of the secret occurred", and
  `DescribeSecretResponse.DeletedDate` is "The date the secret is scheduled
  for deletion". The model does not settle which holds. This bundle chooses
  the first, the time of the `DeleteSecret` request, and answers it from both
  operations; that is an implementation choice, not something the model
  states. The end of the window is `DeleteSecret`'s `DeletionDate`, "the date
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
    Before, the delete went ahead.
- **RestoreSecret** clears `DeletedDate` and answers `ARN` and `Name`.
- **CloudFormation, the one in-repo consumer.**
  - `UpdateStack` updates a secret the stack made with `UpdateSecret`, sending
    only the members the template gives, where it called `CreateSecret` on the
    name again.
  - `CreateSecret` gets `SecretString` and `Description` only when the template
    gives them.
  - A resource the template does not name, of any type, is made under
    `<stack>-<logical id>-<suffix>`, the modelled form: the suffix is twelve
    hex digits of a hash of the stack id and logical id, so no two stacks, and
    no two resources of one stack, share a name. A bucket's name is lower
    case, and a queue with `FifoQueue` true is made a FIFO queue whose name
    ends in `.fifo`. Characters other than letters, digits and hyphens are
    dropped, and the stack name and logical id are cut, never the suffix, to
    fit the service's longest name: 80 for a queue, 64 for a rule, a role or a
    function, 128 for a stream, 255 for a table, 256 for a topic and 63 for a
    bucket. Before, a secret's suffix was the stack id's, and most other types
    were named for the bare logical id (a bucket `<stack>-<logical id>`, a
    parameter and a log group under a path with the stack name), so a second
    stack from one template failed on its table, or took over the first
    stack's queue, topic and rule and deleted them when it rolled back.
  - A name the template gives that a resource the stack did not make already
    holds fails the stack before anything is made, with that service's error
    for a name taken (`QueueAlreadyExists`, `BucketAlreadyOwnedByYou`,
    `EntityAlreadyExists`, `ResourceInUseException` and so on; for a topic,
    whose `CreateTopic` "is idempotent" and has none, CloudFormation's own
    `AlreadyExistsException`, "The resource with the name requested already
    exists."). The resource is left as it was. It is checked again just
    before each create, since a name can come from a resource made before it.
    A REST API is not checked: API Gateway holds no name to one API. Before, a
    create that a service answers with what it already holds (a queue, topic,
    rule, role, stream, log group or bucket here) took the resource over, and
    the stack's rollback or `DeleteStack` deleted it.
  - A stack name "must be unique in the Region in which you are creating the
    stack", so `CreateStack` of a name a stack already has is
    `AlreadyExistsException` ("Stack [name] already exists") and makes
    nothing. Before, it made a second set of resources and wrote its record
    over the first, which then belonged to no stack.
  - A `CreateStack` that fails rolls back what it had created, as
    CloudFormation's default `OnFailure` (`ROLLBACK`) does, deleting a secret
    without a recovery window so its name is free again. A resource is
    recorded the moment it exists, so one whose create failed partway (a
    bucket whose configuration S3 refused, a rule whose targets EventBridge
    refused) is deleted with the rest, and a create that failed outright
    records nothing. `OnFailure` is not read, so every failed create rolls
    back. A failed `UpdateStack` is not rolled back.
  - A failed create keeps its stack, `ROLLBACK_COMPLETE` once the rollback
    has deleted everything it made. The name stays taken (`CreateStack` is
    `AlreadyExistsException`; `UpdateStack` is `ValidationError`, "Stack:...
    is in ROLLBACK_COMPLETE state and can not be updated.") until
    `DeleteStack` removes the stack ("Deleted stacks don't show up in the
    DescribeStacks operation if the deletion has been completed
    successfully"). A resource the rollback cannot delete stays listed by
    `DescribeStackResources` as `DELETE_FAILED`, the stack is
    `ROLLBACK_FAILED`, and the service's error is its `StackStatusReason`
    ("Success/failure message associated with the stack status");
    `DeleteStack` tries the resource again. A `DeleteStack` that cannot delete
    a resource likewise leaves the stack `DELETE_FAILED`, listing what is
    left, and answers the service's error at once, as this emulator's
    `CreateStack` answers a provisioning error, where CloudFormation would
    report it only in the stack's status. A resource already gone (its
    service's not-found error) counts as deleted. Before, a failed create left
    no stack, and `DeleteStack` dropped every delete error and removed the
    stack whatever was left.
  - `GenerateSecretString` makes the value with `GetRandomPassword`'s generator,
    seeded by the stack id and logical id, and puts it under `GenerateStringKey`
    in `SecretStringTemplate` when the template gives one. Its properties are
    read as a template's are: a number or boolean may be given as its text
    (`"16"`, `"true"`), which is how a `Ref` to a parameter gives one. Each
    way it can be wrong is `ValidationError` with its own message: not an
    object; `SecretString` beside it; `GenerateStringKey` without
    `SecretStringTemplate`, or the reverse; a template that is not JSON, or
    not a JSON object; a property of the wrong type or one it does not have,
    named; and parameters no password meets, with the generator's reason.
    Before, such a secret had no value.
- **A secret scheduled for deletion refuses writes.** Every operation that
  writes to a secret answers `InvalidRequestException` ("Possible causes: The
  secret is scheduled for deletion."), which each of them lists among its
  errors, and leaves the secret as it was: `CreateSecret` on its name,
  `PutSecretValue`, `UpdateSecret`, `RotateSecret`, `CancelRotateSecret`,
  `UpdateSecretVersionStage`, `TagResource`, `UntagResource`,
  `PutResourcePolicy`, `DeleteResourcePolicy`, `ReplicateSecretToRegions`,
  `RemoveRegionsFromReplication` and `StopReplicationToReplica`. Before, the
  first four wrote the value and undeleted the secret; `TagResource`,
  `UntagResource`, `UpdateSecretVersionStage`, `PutResourcePolicy`,
  `DeleteResourcePolicy`, `ReplicateSecretToRegions` and
  `RemoveRegionsFromReplication` changed its tags, stages, policy or
  replicas; and `CancelRotateSecret` and `StopReplicationToReplica` succeeded
  without changing anything a read shows. The bundle writes the rule once and
  every one of these uses it except `CreateSecret`, whose refusal has its own
  message ("a secret with this name is already scheduled for deletion"). No
  write operation leaves the error
  out of its list, so none is left unchanged for that reason. `RestoreSecret`
  is the way back, and `DeleteSecret` has its own rules (below). The reads are
  unchanged: `DescribeSecret` and `ListSecretVersionIds` do not list the
  error, and `GetResourcePolicy`, which does, writes nothing.
  `GetSecretValue` refused it already ("When a secret is scheduled for
  deletion, you cannot retrieve the secret value. You must first cancel the
  deletion with RestoreSecret").
- **GetRandomPassword honours its parameters.** It answers `PasswordLength`
  characters (32 when absent) from the documented classes, minus
  `ExcludeCharacters` and any class an `Exclude*` switch drops, and at least
  one of each class unless `RequireEachIncludedType` is false. With
  `IncludeSpace` "the password can contain space characters": a space is
  drawn like any other character but never required, since
  `RequireEachIncludedType` asks for "at least one upper and lowercase
  letter, one number, and one punctuation". Parameters no password satisfies (a
  length outside 1 to 4096, every class excluded, fewer characters than
  required classes) are `InvalidParameterException`. Before, it answered 32
  hex digits whatever it was asked. The generator is seeded, so a seed
  answers one password; CloudFormation's `GenerateSecretString` uses it.
- **Filters choose secrets.** `BatchGetSecretValue` and `ListSecrets` apply
  `Filters` as `Filter.Key` describes them: `name`, `tag-key`, `tag-value` and
  `primary-region` are case-sensitive prefix matches, `description` a prefix
  match that is not case-sensitive, and `all` "Breaks the filter value string
  into words and then searches all attributes for matches. Not
  case-sensitive." A value starting with `!` negates. Every filter must
  match, a filter matches when any of its values does, and with `all` every
  word must start a word of some attribute; the model does not say how
  values combine, so that is this note's reading. `Filter.Key` is an enum
  (`description`, `name`, `tag-key`, `tag-value`, `primary-region`,
  `owning-service`, `all`), so any other key is `ValidationException`, in the
  framework's wording: "1 validation error detected: Value 'nmae' at
  'filters.2.member.key' failed to satisfy constraint: Member must satisfy
  enum value set: [...]". No secret here has an owning service, so
  `owning-service` matches none. Before, both operations ignored `Filters`.
  - `BatchGetSecretValue` refuses `Filters` with `SecretIdList` ("You must
    include Filters or SecretIdList, but not both") and a request with
    neither, with `InvalidParameterException`. The filter form pages with
    `MaxResults` and `NextToken` as `ListSecrets` does, leaves out a secret
    scheduled for deletion as `ListSecrets` does, and answers a secret with no
    value as a not-found `Errors` row. Only the filter form pages:
    `MaxResults` says "To use this parameter, you must also use the Filters
    parameter", so the `SecretIdList` form with `MaxResults`, or with a
    `NextToken`, which only the filter form answers, is
    `InvalidParameterException`, and the `SecretIdList` form never answers a
    `NextToken`. `MaxResults` outside its range, 1 to 20, is
    `ValidationException` in either form. Before, the `SecretIdList` form with
    `MaxResults` answered a `NextToken` whenever more secrets were stored than
    the page held, and every page repeated the same values, so a caller
    following it paged forever.
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
  and both wrote `[]`. `ReplicationStatus` is likewise left out once the last
  replica is removed; `DescribeSecret` wrote `[]`.
- **Timestamps are epoch seconds.** `DeletionDate` and `DeletedDate` are
  timestamps (`DeletionDateType`, `DeletedDateType`) on an `awsJson1_1`
  service with no `timestampFormat` trait, so they are written as epoch
  seconds, a JSON number. On `main`, `DeleteSecret` answered `DeletionDate` as
  a date string, which the Go SDK v2 refuses (it decodes a timestamp only
  from a JSON number), so `DeleteSecret` failed to decode in every Go SDK v2
  client, Terraform's included; `DescribeSecret` refused a scheduled secret
  there and answered no `DeletedDate` at all. The first commit on this branch
  answered `DeletedDate` as a string too, and that is fixed with it. They carry
  milliseconds where the clock has them (`1.5`, `2592001.5`), as the model's
  examples do ("CreatedDate": 1523477145.713); they were whole seconds. Both
  come from one reading of the clock per request, so `DeletionDate` is
  `DeletedDate` plus the window exactly; the engine reads the clock afresh
  for every expression, and they drifted apart by a millisecond.

Equivalence trace `aws.secretsmanager`, steps counted from 0. Step 48, the
pack's delete of `primary`, is refused now, because `primary` has had a
replica in us-west-2 since step 35. Steps 49 to 59 are added. Step 49 removes
the replica and step 50 schedules `primary` for deletion. Each of steps 51 to
59 is then made against the scheduled secret: a `PutSecretValue`, a
`BatchGetSecretValue`, and one each of `UpdateSecretVersionStage`,
`PutResourcePolicy`, `DeleteResourcePolicy`, `ReplicateSecretToRegions`,
`RemoveRegionsFromReplication`, `StopReplicationToReplica` and
`CancelRotateSecret`. Steps 26, 37, 45, 46, 47, 48, 61, 62, 63, 64 and 67 are
re-cut, and every step whose reason names another step was re-cut again to
name it where it now is. Each carries the sentence above that it now follows.
The first commit's re-cuts at 52, 53 and 56 are now at 63, 64 and 67.
