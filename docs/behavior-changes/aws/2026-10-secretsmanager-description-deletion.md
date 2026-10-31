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
- **A secret with no value is not rotated.** `RotateSecret` "starts a rotation
  with the values already stored in the secret". A secret created without a
  value has none, and no rotation function ever runs here to make one, so
  `RotateSecret` of it is `InvalidRequestException`, the model's error for a
  request that is "not valid for the current state of the resource". That is
  this note's reading: the model does not say what happens, and the real
  service would start the rotation function, which would fail. Before, the
  empty string was seated as the secret's first version.
- **Description.** `CreateSecret` and `UpdateSecret` keep `Description`, and
  `DescribeSecret` answers it. Before, it was accepted and dropped. A write
  that names no description keeps the one already stored.
- **Deprecated versions are not in the stage maps.** A version with no staging
  label "is considered deprecated and subject to deletion. Such versions are
  not included in this list" (`SecretListEntry.SecretVersionsToStages`;
  `DescribeSecretResponse.VersionIdsToStages` is "A list of the versions of
  the secret that have staging labels attached"). `ListSecrets` and
  `DescribeSecret` leave such a version out; they answered it with `[]`.
  `ListSecretVersionIds` lists it only with `IncludeDeprecated` ("By
  default, versions without staging labels aren't included"); it listed
  every version.
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
  - A YAML template is read as YAML, with a YAML library: flow collections,
    block scalars and every short-form intrinsic (`!Ref`, `!GetAtt`, `!Sub`,
    and `!Name` for any other `Fn::Name`). The hand-written subset it replaced
    read a flow sequence such as `Tags: [{Key: env, Value: dev}]` as a string
    and refused a block scalar. CloudFormation "doesn't support the following
    features: ... Aliases[,] Hash merges" (User Guide, Template formats), so
    an anchor, an alias or a merge key is `ValidationError`, and a template
    nesting deeper than 100 levels or holding more than 100,000 nodes is
    refused too. Without that, the library's aliases let a template refer to
    itself (`a: &a [*a]`, which overflowed the stack) or multiply (a billion
    laughs, which ran without end).
  - A resource the template does not name, of any type, is made under
    `<stack>-<logical id>-<suffix>`, the modelled form: the suffix is twelve
    hex digits of a hash of the stack id and logical id, so no two stacks, and
    no two resources of one stack, share a name. A bucket's name is lower
    case. A queue with `FifoQueue` true is made a FIFO queue (`FifoQueue` was
    not passed to `CreateQueue` before), and its name, when the engine makes
    it, ends in `.fifo`. Characters other than letters, digits and hyphens are
    dropped from the logical id (a stack name never holds one, below), and the stack name and logical id are cut, never the suffix, to
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
    exists."). The resource is left as it was. A name built on another
    resource's id is checked by the id that resource will be made with: before
    anything is made, each resource's id is planned in the order resources are
    made, so a `Ref` answers what the type's `Ref` will answer (a bucket's
    name, a topic's, role's, rule's, function's or secret's ARN), never the
    logical id. A queue's URL (its endpoint's business), a KMS key's id and a
    REST API's id are known only once made, so a name built on one is checked
    just before its create instead. Resources are made in dependency order,
    each after those its properties name by `Ref`, `Fn::GetAtt` or `Fn::Sub`
    ("Resource B is created before resource A", Template Reference, DependsOn
    attribute), and otherwise by logical id; `DependsOn` itself is not read.
    A cycle is `ValidationError` ("Circular dependency between resources:
    [A, B]") on `CreateStack`, `UpdateStack`, `ValidateTemplate` and
    `GetTemplateSummary`, and makes no stack. Before, a `Ref` to a resource
    not yet made answered its logical id: beside an SSM parameter named
    `ABucket`, a parameter named for an unnamed bucket `ABucket` was refused
    as taken, and a resource that sorted before the one it named was made
    with the logical id, and a cycle was made in alphabetical order.
    A REST API is not checked: API Gateway holds no name to one API. A stack
    recorded before resources kept the name they were made under is read by
    each resource's physical id, which is the name or ends in it (a queue's
    URL, a secret's ARN), so such a stack still updates. Before, a
    create that a service answers with what it already holds (a queue, topic,
    rule, role, stream, log group or bucket here) took the resource over, and
    the stack's rollback or `DeleteStack` deleted it.
  - EventBridge keys a rule by its event bus and its name, and the rule's
    ARN carries the bus (`rule/<bus>/<name>`), so the check of a rule's name,
    its `PutTargets` and its delete all carry the bus: the `EventBusName`
    property, or the bus in the ARN. A named rule already on bus `b` fails
    the stack and is left as it was, a rule of that name on the default bus
    does not block it, and `DeleteStack` deletes the stack's own rule on its
    bus, never a same-named rule on the default bus. Before, all three
    looked at the default bus.
  - A stack name "must be unique in the Region in which you are creating the
    stack", so `CreateStack` of a name a stack already has is
    `AlreadyExistsException` ("Stack [name] already exists") and makes
    nothing. The name is reserved, a `CREATE_IN_PROGRESS` record written in
    the same store transaction that found it free, before any resource is
    made, so of concurrent creates of one name exactly one makes anything.
    Before, it made a second set of resources and wrote its record over the
    first, which then belonged to no stack, and creates racing on one name
    each made a set.
  - A `CreateStack` that fails rolls back what it had created, as
    CloudFormation's default `OnFailure` (`ROLLBACK`) does, deleting a secret
    without a recovery window so its name is free again. A resource is
    recorded the moment it exists, so one whose create failed partway (a
    bucket whose configuration S3 refused, a rule whose targets EventBridge
    refused) is deleted with the rest, and a create that failed outright
    records nothing. The rollback deletes only what this create made: no two
    stacks share a name the engine makes, and a given name another resource
    already holds is refused before anything is made (above). `OnFailure` is
    not read, so every failed create rolls back.
  - A stack name follows `CreateStack`'s rule: "A stack name can contain
    only alphanumeric characters (case sensitive) and hyphens. It must start
    with an alphabetical character and can't be longer than 128 characters."
    Any other name is `ValidationError` in the framework's wording ("1
    validation error detected: Value 'under_score' at 'stackName' failed to
    satisfy constraint: Member must satisfy regular expression pattern:
    [a-zA-Z][-a-zA-Z0-9]*", or "Member must have length less than or equal to
    128"), and nothing is made. `UpdateStack`, `DeleteStack` and
    `DescribeStacks` take "The name or unique stack ID", so they check the
    same pattern with a stack id admitted. Before, any name was taken.
  - Every operation that takes "The name or unique stack ID" finds a stack by
    its `StackId` (`arn:...:stack/<name>/<id>`) as well as by its name, and
    only while the stack of that name has that id: `DescribeStacks`,
    `DescribeStackResources`, `ListStackResources`, `DescribeStackEvents`,
    `DescribeStackResource`, `GetTemplate`, `GetTemplateSummary`,
    `UpdateTerminationProtection`, the rollback family, `UpdateStack`,
    `DeleteStack` and the change sets. Stacks stay keyed by name; no record is
    keyed by an id. `UpdateStack`, and `ExecuteChangeSet` of an `UPDATE`
    change set (the default `ChangeSetType`, which `CreateChangeSet` now
    records), of a stack that does not exist is `ValidationError`, "Stack [x]
    does not exist", and makes nothing. Before, the reads did not find a stack
    by its id, an update of a missing stack created it, and an `UpdateStack`
    by id made a second stack keyed by the id.
  - Where the model's input uses `StackNameOrId` (`ContinueUpdateRollback`,
    `CreateChangeSet`, `DeleteChangeSet`, `DescribeChangeSet`,
    `DescribeChangeSetHooks`, `DescribeEvents`,
    `DescribeStackResourceDrifts`, `DetectStackDrift`,
    `DetectStackResourceDrift`, `ExecuteChangeSet`, `GetTemplateSummary`,
    `ListChangeSets`, `RollbackStack`, `SignalResource`,
    `UpdateTerminationProtection`), the name is held to its length (1 or
    more) and the model's pattern,
    `^([a-zA-Z][-a-zA-Z0-9]*)|(arn:\b(aws|aws-us-gov|aws-cn)\b:[-a-zA-Z0-9:/._+]*)$`,
    each alternative matched whole, so a bare `arn:`, or an id in another
    partition, is `ValidationError` in the framework's wording. The reads
    that take a name or id (`DescribeStacks` and the rest above) and
    `UpdateStack` and `DeleteStack` check the same pattern. Before, those
    operations took any name, and the reads admitted any `arn:` text.
  - "Before you can delete the rule, you must remove all targets, using
    RemoveTargets" (EventBridge, `DeleteRule`), and `Force` "is ignored for
    rules that are not managed rules". So EventBridge's `DeleteRule` of a rule
    that still has targets is `ValidationException` ("Rule can't be deleted
    since it has targets.") and deletes nothing, and CloudFormation removes a
    rule's targets with `RemoveTargets` before it deletes the rule, on
    `DeleteStack`, on a rollback and on an update's cleanup, and no longer
    passes `Force`. A later rule of that name starts with no targets. The
    EventBridge trace gains a `RemoveTargets` of `r1`'s last target before its
    delete (step 50). Before, `DeleteRule` deleted a rule and kept its
    targets, which a new rule of the name inherited.
  - A rule whose targets EventBridge did not all add fails. `PutTargets` "can
    partially fail ... FailedEntryCount is non-zero in the response and each
    entry in FailedEntries provides the ID of the failed target and the error
    code", so the rule is not the one the template declares. The resource's
    error is the first entry's code, with a message naming every failed entry,
    and the stack rolls back, on create and on update. Before, the stack
    finished `CREATE_COMPLETE` or `UPDATE_COMPLETE` without those targets.
  - "The Resources section is required in every CloudFormation template"
    (User Guide, Template sections), so a template without one is
    `ValidationError`, "Template format error: At least one Resources member
    must be defined.", on `CreateStack`, `UpdateStack`, `ValidateTemplate`
    and `GetTemplateSummary`. Before, such a stack was created.
  - A failed create keeps its stack, `ROLLBACK_COMPLETE` once the rollback
    has deleted everything it made, with the create's error as its
    `StackStatusReason` (which `ListStacks` answers too, as the model's
    `StackSummary` has it), and every read of it answers it with no
    resources (the record never holds a null list). The name stays taken (`CreateStack` is
    `AlreadyExistsException`; `UpdateStack` is `ValidationError`, "Stack:...
    is in ROLLBACK_COMPLETE state and can not be updated.") until
    `DeleteStack` removes the stack ("Deleted stacks don't show up in the
    DescribeStacks operation if the deletion has been completed
    successfully"). A resource the rollback cannot delete stays listed by
    `DescribeStackResources` as `DELETE_FAILED`, with the error as its
    `ResourceStatusReason` ("Success/failure message associated with the
    resource"); `ListStackResources`, `DescribeStackResource` and
    `DescribeStackEvents` answer the same status and reason from the same
    record, where events said `CREATE_COMPLETE` whatever happened. The stack is
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
  - A failed `UpdateStack` rolls back as `UPDATE_ROLLBACK` does: what it
    created is deleted, what it changed is provisioned again from the
    previous template (a secret gets its previous value and description back
    through `UpdateSecret`, as a new version), and the stack keeps its
    previous template and resources as `UPDATE_ROLLBACK_COMPLETE`. A failed
    update has deleted nothing old (its cleanup runs only after a success,
    below), so none needs recreating. What the update made is recorded the moment it exists, so a half-made one is
    deleted too. A resource the rollback cannot delete stays listed by
    `DescribeStackResources` as `DELETE_FAILED`, the stack is
    `UPDATE_ROLLBACK_FAILED` with the error as its `StackStatusReason` (an
    `UpdateStack` of it is `ValidationError`, as of any stack in a
    `ROLLBACK_` or `_FAILED` state), and `DeleteStack` removes the resource
    with the rest. Before, the changes stayed and the new resources were left
    behind.
  - An update compares each resource's type and resolved properties with
    what the stack recorded ("CloudFormation updates resources based on
    differences between what you submit and the stack's current template",
    Template Reference, UpdateReplacePolicy). An unchanged resource is not
    touched: no call, and the same physical id. A change to properties the
    type updates in place ("Update requires: No interruption" or "Some
    interruptions" on its Template Reference page) is made through its
    service's update under the same id: `UpdateKeyDescription`,
    `PutParameter` with `Overwrite`, `UpdateTable`,
    `UpdateAssumeRolePolicy`, `UpdateFunctionConfiguration`,
    `UpdateShardCount`, `UpdateSecret`, `UpdateRestApi`, `PutRule` with the
    rule's targets set to the template's (others removed), and a bucket's
    configuration; a queue, topic or log group has no property the engine
    applies but its name, so a change to another of theirs calls nothing. A
    change to a property that "Update requires: Replacement" (a name,
    `FifoQueue`, `FifoTopic`, a role's `Path`, a key's `KeySpec`,
    `KeyUsage` or `MultiRegion`, a rule's `EventBusName`, a function's
    `PackageType` and so on) makes a new resource, under a new generated
    name, and the cleanup deletes the old one; a resource whose name the
    template gives and keeps is `ValidationError`, CloudFormation's
    "CloudFormation cannot update a stack when a custom-named resource
    requires replacing. Rename <name> and update the stack again.", and the
    stack rolls back. A `Ref` to a kept resource answers its id. Before, every
    update made every resource again: a new KMS key and REST API, whose old
    ones the cleanup then deleted, and a failure on any parameter, table, log
    group, function, role or stream, which could only be created. A stream
    is made with the template's `ShardCount`.
  - A failed update's rollback puts back exactly the properties the stack
    had: what the update changed in place is updated back through the same
    update, given the previous template's properties, so a property the
    failed update added is taken away (a rule's targets are its previous
    template's and no others; a secret given a description has an empty one,
    which Secrets Manager no longer answers, as it "only returns fields that
    have a value"); what the update kept is not called. Before, the rollback
    provisioned the previous properties on top of the new ones, and every
    kept resource again.
  - `DeletionPolicy` and `UpdateReplacePolicy` are read as the Template
    Reference gives them (the model's `TemplateConfiguration` names the same
    `DELETE` and `RETAIN`). `Retain` "keeps the resource without deleting the
    resource or its contents": on `DeleteStack`, on an update that drops the
    resource ("it's deleted from CloudFormation's scope"), and on the rollback
    of the create, or of the update, that made it, since
    `RetainExceptOnCreate` is the one that "deletes the resource" when "the
    stack operation that created the resource is rolled back" and otherwise
    retains it. `UpdateReplacePolicy: Retain` keeps the old resource a
    replacement leaves; its `Snapshot` "reverts to the default option, which
    is Delete" for a type with no snapshots, which is every type the engine
    makes. `DeletionPolicy: Snapshot` is for the types the page lists, none
    of them the engine's, so it is refused, as is a value neither attribute
    has: `ValidationError`, "Template format error: Q's DeletionPolicy must
    be one of [Delete, Retain, RetainExceptOnCreate], not "Snapshot"" (this
    wording is the emulator's). A retained resource leaves the stack and is
    listed by `DescribeStackEvents` as `DELETE_SKIPPED`, the model's
    `ResourceStatus`. A deleted stack's record stays, `DELETE_COMPLETE`,
    answered by its stack id alone (`DescribeStacks`, `DescribeStackEvents`
    and the other reads; `ListStacks` lists it), and exports nothing; by its
    name it does not exist, and the name is free. Before, neither attribute
    was read, every resource was deleted, and a deleted stack was gone.
  - A finished `UpdateStack` cleans up. "CloudFormation will replace the
    resource by creating a new one and deleting the old one"
    (`ResourceChange.Replacement`), and for an update "it begins disposing of
    old resources immediately once the operation completes"
    (`RollbackConfiguration`). Once every resource of the update is in place,
    each old resource the stack no longer holds is deleted: one the update
    replaced with a new physical id (a new name, say) and one the template
    dropped. A delete that fails there leaves the stack `UPDATE_COMPLETE`, as
    in CloudFormation, but is not lost: the old resource stays listed by
    `DescribeStackResources` as `DELETE_FAILED`, its `ResourceStatusReason`
    ("Success/failure message associated with the resource") the error, which
    is also the stack's `StackStatusReason`, and `DeleteStack` deletes it with
    the rest. Stack statuses here have no `_IN_PROGRESS` grain, so
    `UPDATE_COMPLETE_CLEANUP_IN_PROGRESS` is never seen. Before, an update
    never deleted what it replaced or dropped.
  - The rollback family accepts only the states the API reference gives
    each operation: `RollbackStack` rolls back "from CREATE_FAILED or
    UPDATE_FAILED stack statuses", `ContinueUpdateRollback` "from
    UPDATE_ROLLBACK_FAILED to UPDATE_ROLLBACK_COMPLETE state", and
    `CancelUpdateStack` "only stacks that are in the UPDATE_IN_PROGRESS
    state". Nothing here runs asynchronously and a failed create rolls back at
    once, so no stack rests in the first, second or last of those states:
    every call on another stack, a `ROLLBACK_FAILED` one with its
    `DELETE_FAILED` orphan included (its way forward is `DeleteStack`), is
    `ValidationError` ("RollbackStack cannot be called from current stack
    status", in the words `CancelUpdateStack` uses) and changes nothing, and
    an absent stack "does not exist". `ContinueUpdateRollback` never marks a
    stack complete while a `DELETE_FAILED` resource remains, since it cannot
    retry the delete. Before, each operation marked any stack complete,
    orphans and all, and made a stack out of a name that had none. The
    CloudFormation trace's steps 9 to 22 are re-cut accordingly: s1 and s2
    are never made.
  - `GenerateSecretString` makes the value with `GetRandomPassword`'s generator,
    seeded by the stack id and logical id, and puts it under `GenerateStringKey`
    in `SecretStringTemplate` when the template gives one. Its properties,
    and every other property the engine reads, are read by one rule as the
    type the template reference gives them, the value itself or its text,
    which is how a `Ref` to a parameter gives one. A Boolean is `true` or
    `false` in any case (YAML 1.1, the templates' YAML, spells it `true`,
    `True` or `TRUE`), so `FifoQueue: "True"` makes a FIFO queue; an Integer
    is decimal digits, as a Number parameter's text is, so `"016"` is 16 and
    `"16.0"`, `"1e1"`, `"+16"` and `" 16"` are refused; a String is a
    scalar's text, so `ExcludeCharacters: 123` is `"123"` and an SSM
    Parameter's `Value: 123` is `"123"`. Before, `"True"` made a standard
    queue, `"16.0"`, `"1e1"` and `"+16"` were read as numbers, a number was
    refused as `ExcludeCharacters` and an SSM `Value: 123` was empty. A
    parameter is a string ("the parameter value becomes a string", User
    Guide, Parameters), so a numeric `Default`, `Default: 16` in JSON or
    YAML, is its text `"16"`; before, it became `""`. Each
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
    parameter", so the `SecretIdList` form with `MaxResults` is
    `InvalidParameterException`, and that form never answers a `NextToken`.
    Of a `NextToken` beside `SecretIdList` the API reference says nothing
    specific; no listing of that form answers one, so it is
    `InvalidNextTokenException` ("The NextToken value is invalid."), which the
    operation lists. `SecretIdList` holds "Minimum number of 1 item. Maximum
    number of 20 items.", so an empty list (beside `Filters` too) or 21 ids is
    `ValidationException`, ahead of the rules on the two forms, and
    `ListSecrets`' `MaxResults` outside 1 to 100 is `ValidationException`. `MaxResults` outside its range, 1 to 20, is
    `ValidationException` in either form. `MaxResults` is an integer ("Type:
    Integer", in `ListSecrets` too), so a value the awsJson1_1 codec cannot
    read as one (`"abc"`, `"5"`, `1.5`, `true`) is `SerializationException`,
    as a fractional `RecoveryWindowInDays` is; before, `"abc"` was an internal
    error and `"5"` and `1.5` were taken. Before, the `SecretIdList` form with
    `MaxResults` answered a `NextToken` whenever more secrets were stored than
    the page held, and every page repeated the same values, so a caller
    following it paged forever.
  - A listing page that keeps nothing while more secrets follow now moves
    `NextToken` past what it read; before, `ListSecrets` with a scheduled
    secret between two listed ones lost the second (an engine fix).
- **CreateSecret keeps its Tags** ("A list of tags to attach to the
  secret"), as `TagResource` keeps them, so `DescribeSecret` answers them, the
  `tag-key` and `tag-value` filters find the secret and a later `TagResource`
  adds to them. Before, `CreateSecret` dropped them.
- **BatchGetSecretValue leaves out a secret scheduled for deletion.** Its value
  is not answered; it is an `Errors` row, an `APIErrorType` of `SecretId`,
  `ErrorCode` `InvalidRequestException` and `Message`. Before, its value was
  answered in `SecretValues`. Every `Errors` row carries the code and message
  `GetSecretValue` answers for that secret alone: a secret that does not
  exist is `ResourceNotFoundException`, "Secrets Manager can't find the
  resource that you asked for." (the model's text for the exception, which
  every `ResourceNotFoundException` of this service now carries), and a
  secret with no value "Secrets Manager can't find the specified secret value
  for staging label: AWSCURRENT". `GetSecretValue` of a scheduled secret
  answers the same message as the other refused calls. Before, a not-found
  row had no `Message` and the single call's was empty.
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
`CancelRotateSecret`. Steps 17, 24, 26, 37, 45, 46, 47, 48, 61, 62, 63, 64 and 67 are
re-cut, and every step whose reason names another step was re-cut again to
name it where it now is. Each carries the sentence above that it now follows.
The first commit's re-cuts at 52, 53 and 56 are now at 63, 64 and 67.
