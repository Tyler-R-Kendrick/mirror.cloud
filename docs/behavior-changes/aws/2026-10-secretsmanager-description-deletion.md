# Secrets Manager: description and scheduled deletion (2026-10)

The oracle is AWS's documentation as it ships in the vendored model
(`secrets-manager-2017-10-17.json`, sha256 `2738bd8d…`, pinned in
`specs/mirror.lock`).

- **CreateSecret creates; the writes need a secret.** `CreateSecret` of a name
  that exists is `ResourceExistsException` ("A resource with the ID you
  requested already exists"). `PutSecretValue`, `UpdateSecret` and
  `RotateSecret` of a name never created are `ResourceNotFoundException`
  ("Secrets Manager can't find the specified secret."), which each
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
  empty string was seated as the secret's first version. The same holds for
  a secret whose versions hold no `AWSCURRENT` stage (removed with
  `UpdateSecretVersionStage`), and the refusal says so: "The secret has no
  value to rotate: it has no AWSCURRENT version." checks for that stage, not
  for any version.
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
    an alias or a merge key is `ValidationError`. An anchor no alias refers
    to is read as if it were not there, as the sentence names aliases and
    hash merges and no anchor (a reading; round 7 refused every anchor).
    The same sentence's "binary, omap, pairs, set, and timestamp tags",
    written on a node, are each `ValidationError` naming the tag; a date
    written plainly is not a tag and stays a string
    (`AWSTemplateFormatVersion: 2010-09-09`). Before, each tag was read.
    Without that, the
    library's aliases let a template refer to itself (`a: &a [*a]`, which
    overflowed the stack) or multiply (a billion laughs, which ran without
    end).
  - "CloudFormation supports the YAML Version 1.1 specification" (User Guide,
    Template formats), so a plain scalar is read as YAML 1.1's type
    repository reads it (yaml.org/type/bool.html, int.html, float.html),
    where the library reads YAML 1.2: `yes`, `no`, `on` and `off` in their
    listed spellings are booleans, and `y` and `n` stay strings, as PyYAML
    and SnakeYAML leave them though the type page lists them (a reading;
    round 8 read them as booleans); an integer may be binary, octal
    as `0777`, hexadecimal or sexagesimal (`1:20` is 80), with underscores,
    but `0o17` and `08` are strings; a float has a point, so `1e3` is the
    string `1e3`. `.inf` and `.nan` stay text, which a template's JSON form
    could not hold (a reading). A quoted scalar is a string, and an explicit
    tag (`!!str yes`) is read as tagged. Before, `yes` was the string
    `"yes"` and made `FifoQueue: yes` a standard queue, `1:20` was a string,
    `1e3` was 1000 and `0o17` was 15.
  - "Resources: The maximum number of resources that you can declare in your
    CloudFormation template. 500 resources" (User Guide, quotas): a template
    of 501 is `ValidationError`, "Template format error: Number of
    resources, 501, is greater than maximum allowed, 500" (the wording is a
    reading). Before, any number was read.
  - A template is bounded by its size, CloudFormation's own limits:
    `TemplateBody` is "a maximum length of 51,200 bytes", and the template
    `TemplateURL` names "max size: 1 MB" (API reference, `CreateStack`;
    "Template body size in a request" and "... in an Amazon S3 object", User
    Guide, Understand CloudFormation quotas). A longer body, YAML or JSON, is
    `ValidationError`, the member's constraint error ("Member must have length
    less than or equal to 51200"), on `CreateStack`, `UpdateStack`,
    `ValidateTemplate`, `GetTemplateSummary` and `CreateChangeSet`; an S3
    object over 1,000,000 bytes is `ValidationError` too. That the quotas'
    1 MB is 1,000,000 bytes is a reading: the page writes 204,800 bytes as
    "204.8 KB". Neither page gives the error's wording. Depth is bounded by
    the parsers, which refuse nesting past 10,000 levels (`exceeded max
    depth`), as `ValidationError`. A round-7 bound of 100 levels and 100,000
    nodes refused real templates (200 mappings of 100 keys of two attributes,
    642 KB, within the quotas) and is gone.
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
    made, so a `Ref` answers what the type's `Ref` will answer, never the
    logical id.
  - A `Ref` answers what each type's Template Reference page says its `Ref`
    returns, and `Fn::GetAtt` each attribute the page lists, derived from it:

    | Type | `Ref` | `Fn::GetAtt` |
    | --- | --- | --- |
    | `AWS::S3::Bucket` | bucket name | `Arn`, `DomainName` |
    | `AWS::SQS::Queue` | queue URL | `Arn`, `QueueName`, `QueueUrl` |
    | `AWS::SNS::Topic` | topic ARN | `TopicArn`, `TopicName` |
    | `AWS::DynamoDB::Table` | table name | `Arn` |
    | `AWS::IAM::Role` | role name (was its ARN) | `Arn` |
    | `AWS::SSM::Parameter` | parameter name | `Arn` |
    | `AWS::SecretsManager::Secret` | secret ARN | `Id` |
    | `AWS::KMS::Key` | key id | `Arn`, `KeyId` |
    | `AWS::Logs::LogGroup` | log group name | `Arn` |
    | `AWS::Events::Rule` | "event rule ID": the name, `<bus>\|<name>` on another bus (was its ARN) | `Arn`, `RuleName` |
    | `AWS::Lambda::Function` | function name (was its ARN) | `Arn` |
    | `AWS::Kinesis::Stream` | stream name | `Arn` |
    | `AWS::ApiGateway::RestApi` | REST API id | `RestApiId` |
    | `AWS::Kinesis::ResourcePolicy` | the stream ARN it attaches to (its page lists no `Ref`; a reading) | none |

    `<bus>|<name>` for a rule on another bus is a reading: the page's example
    is a default-bus rule. `PhysicalResourceId` is the `Ref`'s id, and
    `DeleteStack` deletes each resource by it, and by the ARN a stack
    recorded before this change holds. An attribute the table does not list
    answers the `Ref`'s value, as before (a REST API's `RootResourceId`, a
    role's `RoleId`, a table's `StreamArn`). Before, `GetAtt` of `Arn`
    guessed from the id's shape, so a table's, parameter's, log group's,
    stream's and key's `Arn` was `arn:aws:s3:::<id>`. A queue's URL (its endpoint's business), a KMS key's id and a
    REST API's id are known only once made, so a name built on one is checked
    just before its create instead. Resources are made in dependency order,
    each after those its properties name by `Ref`, `Fn::GetAtt` or `Fn::Sub`
    ("Resource B is created before resource A", Template Reference, DependsOn
    attribute), and after the resources its `DependsOn` names ("that
    resource is created only after the creation of the resource specified in
    the DependsOn attribute", which "can take a single string or list of
    strings"), and otherwise by logical id. A `DependsOn` naming no resource
    the template declares is "Template format error: Unresolved resource
    dependencies [Nope] in the Resources block of the template", and a cycle
    through it is the cycle's error (below). Before, `DependsOn` was not read.
    `Fn::Sub` substitutes every form its Template Reference gives:
    `${Name.Attribute}` is that resource's `Fn::GetAtt`, a name its variable
    map defines is the variable's value, and `${!Literal}` is the literal
    `${Literal}`; each orders by what it names (a resource named by
    `${Name.Attribute}` or by a variable's value is made first, one a variable
    or `${!...}` merely spells is not). Before, `${Name.Attribute}` and the
    variable map were left as written and ordered nothing. A variable "can be template
    parameter names, resource logical IDs, resource attributes, or a variable
    in a key-value map", and a pseudo parameter; any other, `${Nope}`, is
    `ValidationError`, "Template format error: Unresolved resource
    dependencies [Nope] in the Resources block of the template" (round 8 left
    it in the string).
    A `Fn::If` whose condition the parameters settle orders by the branch it
    takes and only that one: `Fn::Equals` of literals and `Ref`s to
    parameters, and `Fn::Not`, `Fn::And`, `Fn::Or` and `Condition` over such
    conditions (Template Reference, condition functions), each condition
    evaluated once per operation however many name it (round 8 evaluated a
    named condition afresh wherever it was named, so a chain of 24
    conditions each naming the last twice took 19 s). A condition they do
    not settle (one on a pseudo parameter, or one that names itself) orders
    by both branches, a reading. A settled `Fn::If` also resolves to its branch,
    and `AWS::NoValue` there "Removes the corresponding resource property"
    (an element of a list too); an unsettled one reaches the service as
    written, a reading. `AWS::StackId` "Returns the ID (ARN) of the stack"
    (User Guide, pseudo parameters). Round 8 resolved no `Fn::If`, so its
    map reached the service, and left `AWS::StackId` as written. Before, every `Fn::If` ordered by
    both branches, so two resources naming each other only in branches never
    both taken were refused as a cycle.
    The order is found once per operation, by one walk of each resource's
    properties for the names they refer to and Kahn's algorithm over them,
    so it costs time linear in the template; round 7's resolved every
    unordered resource each time it placed one, 125,250 walks to validate
    500 chained parameters and 250,500 to make them.
    A cycle is `ValidationError` ("Circular dependency between resources:
    [A, B]") on `CreateStack`, `UpdateStack`, `ValidateTemplate` and
    `GetTemplateSummary`, and makes no stack. It names the resources in a
    cycle (one naming itself is a cycle of one), not those that wait on one,
    found in one pass of Tarjan's strongly connected components; round 8
    searched once per resource left, quadratic in the template. Before, a `Ref` to a resource
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
    id carries the bus (`<bus>|<name>`, its ARN `rule/<bus>/<name>`), so the check of a rule's name,
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
  - `CreateChangeSet` refuses the stacks `UpdateStack` refuses, a
    `ROLLBACK_*` or `*_FAILED` one too (round 8 refused only one in
    progress), and `GetTemplateSummary` reads the stored template only when
    the request gives none, so an oversize `TemplateBody` beside a
    `StackName` is the member's constraint error.
  - A stack in any `_IN_PROGRESS` state takes no update: `UpdateStack`,
    `CreateChangeSet` and `ExecuteChangeSet` are `ValidationError`,
    "Stack:<id> is in CREATE_IN_PROGRESS state and can not be updated.", the
    words used for a `ROLLBACK_COMPLETE` stack. `DeleteStack` follows its API
    reference, "Once the call completes successfully, stack deletion starts":
    of a stack being created it answers at once and leaves the stack
    `DELETE_IN_PROGRESS` ("Ongoing removal of one or more stacks", User Guide,
    stack status codes), its name still taken; the create, whose last write is
    a compare-and-set on its reservation (the stack's id, still
    `CREATE_IN_PROGRESS`), finds the reservation changed, deletes everything it
    made, and then the stack. A `DeleteStack` of a stack already
    `DELETE_IN_PROGRESS` is answered and changes nothing; that it is not
    refused is a reading. Every write of the record after the reservation, a
    rollback's and a delete's included, is a compare-and-set on what its
    writer read, so a create that finds its stack deleted under it writes
    nothing back. An update reserves nothing, as here an update has no
    `UPDATE_IN_PROGRESS` state for `CancelUpdateStack` to act on, but writes
    only over the record it read (termination protection aside, which it
    keeps: an `UpdateStack` or `ExecuteChangeSet` of a protected stack leaves
    it protected, where round 7's wrote a record without it); one that finds the stack changed by another update or a
    `DeleteStack` deletes what it made that the stack does not now hold (all
    of it when the stack is being deleted) and is `ValidationError`, "...is in
    UPDATE_IN_PROGRESS state..." or "...DELETE_IN_PROGRESS...", naming the
    change that came first as in progress throughout, a reading. Before, a
    `DeleteStack` during a create removed the empty reservation, a second
    `CreateStack` of the name made a second set of resources, the first
    create's last write brought its stack back over the second's record, and
    a final delete left the second set behind (300 parameters in the round-8
    repro); and the later of two updates wrote over the earlier's record,
    whose resources then belonged to no stack.
  - The stack record has no writer but the engine, each write one store
    transaction: `UpdateTerminationProtection` is native and sets the
    protection in a transaction over the record it reads (its Boolean in any
    case, so `TRUE` protects), as `ContinueUpdateRollback` and every other
    native write already did. Before, the bundle's `UpdateTerminationProtection` read the
    record and wrote it back separately, so a create that finished between
    the two was written back `CREATE_IN_PROGRESS`, and a `DeleteStack` then
    left it `DELETE_IN_PROGRESS` for good; `TRUE` left a stack unprotected.
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
  - A `Ref` or `Fn::GetAtt` in the Resources block must name a resource the
    template declares, or, for a `Ref`, a parameter it declares or a pseudo
    parameter; one that names neither is CloudFormation's `ValidationError`,
    "Template format error: Unresolved resource dependencies [Q] in the
    Resources block of the template", every such name listed, on
    `CreateStack`, `UpdateStack`, `ValidateTemplate` and
    `GetTemplateSummary`, and a change set of such a template is `FAILED`
    (below); nothing is made. Before, such a `Ref` answered its own text.
  - A change set's template is parsed and validated when `CreateChangeSet`
    makes it, as CloudFormation does: the call answers the change set's `Id`,
    and a template that is wrong leaves the change set `FAILED` with the
    error as its `StatusReason` ("if your attempt to create a change set
    failed, CloudFormation shows the error message"), which
    `DescribeChangeSet` and `ListChangeSets` answer. `ExecuteChangeSet` of a
    `FAILED` change set is `InvalidChangeSetStatus` (the model's
    `InvalidChangeSetStatusException`, "The specified change set can't be
    used to update the stack"; the message is the emulator's) and changes
    nothing. `CreateChangeSet` is the engine's now; it still takes the
    template as `TemplateBody` only, as the bundle did. The CloudFormation trace's `cs1` has no Resources, so its
    `ListChangeSets` summary (step 26) is re-cut to `FAILED`. Before, the body
    was stored unread, the change set was `CREATE_COMPLETE`, and only
    `ExecuteChangeSet` found the error.
  - "The Resources section is required in every CloudFormation template"
    (User Guide, Template sections), so a template without one is
    `ValidationError`, "Template format error: At least one Resources member
    must be defined.", on `CreateStack`, `UpdateStack`, `ValidateTemplate`
    and `GetTemplateSummary`. Before, such a stack was created.
  - A failed create keeps its stack, `ROLLBACK_COMPLETE` once the rollback
    has deleted everything it made, with the create's error as its
    `StackStatusReason` (which `ListStacks` answers too, as the model's
    `StackSummary` has it), and every read of it answers it with no
    resources. No record holds a null list: the reservation a create writes
    first, a stack with no outputs and a rolled-back stack each list
    `Resources` and `Outputs` as `[]` (round 7 claimed this, and the
    reservation and a stack with no outputs held null). The name stays taken (`CreateStack` is
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
    in CloudFormation, and abandons the resource, as CloudFormation does: it
    leaves the stack's resources, and is recorded once, as a `DELETE_FAILED`
    event with the error as its `ResourceStatusReason` ("Success/failure
    message associated with the resource"), and as the stack's
    `StackStatusReason`. No later update or `DeleteStack` touches it, and no
    CloudFormation delete touches a physical resource another stack's record
    holds: such a one leaves the stack `DELETE_SKIPPED`, its reason naming
    the holder. Before, the abandoned resource stayed listed and every later
    update retried its delete, which, once it was gone and another stack had
    made one of its name, deleted the other stack's. Stack statuses here have no `_IN_PROGRESS` grain, so
    `UPDATE_COMPLETE_CLEANUP_IN_PROGRESS` is never seen. Before, an update
    never deleted what it replaced or dropped.
  - The rollback family accepts only the states the API reference gives
    each operation: `RollbackStack` rolls back "from CREATE_FAILED or
    UPDATE_FAILED stack statuses", `ContinueUpdateRollback` "from
    UPDATE_ROLLBACK_FAILED to UPDATE_ROLLBACK_COMPLETE state", and
    `CancelUpdateStack` "only stacks that are in the UPDATE_IN_PROGRESS
    state". A failed create rolls back at once and an update reserves no
    record, so no stack rests in `CREATE_FAILED`, `UPDATE_FAILED` or
    `UPDATE_IN_PROGRESS`, and neither `RollbackStack` nor `CancelUpdateStack`
    could carry out what it says: each refuses every stack, a record in a
    state it accepts included (round 7 answered those with a success that
    changed nothing). Every call on another stack, a `ROLLBACK_FAILED` one with its
    `DELETE_FAILED` orphan included (its way forward is `DeleteStack`), is
    `ValidationError` ("RollbackStack cannot be called from current stack
    status", in the words `CancelUpdateStack` uses) and changes nothing, and
    an absent stack "does not exist". `ContinueUpdateRollback` is the
    engine's: from `UPDATE_ROLLBACK_FAILED` it deletes again what the
    rollback could not delete and updates back what it could not restore
    (left `UPDATE_FAILED`), but the `ResourcesToSkip`, which "CloudFormation
    sets ... to UPDATE_COMPLETE" and which may name only "resources that are
    in the UPDATE_FAILED state because a rollback failed" (any other is
    `ValidationError`). Success is `UPDATE_ROLLBACK_COMPLETE`, and the stack
    can then be updated; a retry that fails leaves it
    `UPDATE_ROLLBACK_FAILED` with the error as its reason. Before, each operation marked any stack complete,
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
    `True` or `TRUE`), so `FifoQueue: "True"` makes a FIFO queue, and a
    `FifoQueue` that is no Boolean (`maybe`, `1`, a list) is
    `ValidationError` naming it, before anything is made (round 7 made a
    standard queue); an Integer
    is decimal digits after an optional minus, as a Number parameter's text
    is, so `"-5"` is -5 (round 8 refused it), `"016"` is 16 and
    `"16.0"`, `"1e1"`, `"+16"` and `" 16"` are refused; a String is a
    scalar's text, so `ExcludeCharacters: 123` is `"123"` and an SSM
    Parameter's `Value: 123` is `"123"`. Before, `"True"` made a standard
    queue, `"16.0"`, `"1e1"` and `"+16"` were read as numbers, a number was
    refused as `ExcludeCharacters` and an SSM `Value: 123` was empty.
    Every property the engine passes to a service, the ones it passes whole
    included (a function's, a table's), is read by the same rule, keyed by
    the type the operation's model declares for the member, through
    structures, lists and maps. So `MemorySize: !Ref Memory` is
    256, `ReadCapacityUnits` from a parameter is 5, and a bucket's tag
    values written `true` and `123` are `"true"` and `"123"`. Before, a
    function got `MemorySize` `"256"`, a table `"5"`, and the tags were
    `MalformedXML`. A member the model does not declare, and a value that is
    not its member's type, is passed as given, for the service to refuse.
    Round 7 said every property was read as its type; only the ones the
    engine named were. A
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
  - A `NextToken` is only one a listing answered: `ListSecrets` and
    `BatchGetSecretValue`'s `Filters` form answer an opaque token (base64 of
    `secret:` and the last name listed), and a token neither answered is
    `InvalidNextTokenException`, "The NextToken value is invalid.". An empty
    one breaks `NextTokenType`'s "Minimum length of 1" and is
    `ValidationException`, the framework's constraint error, on those two
    and `ListSecretVersionIds`. `ListSecretVersionIds`' `MaxResults` is a
    `MaxResultsType`, a whole number from 1 to 100, with `ListSecrets`'
    checks. The token's form is this bundle's; the model says only that
    callers pass back what they were given. Before, a garbage token was read
    as a name to list after, an empty one as none, and
    `ListSecretVersionIds` took any `MaxResults`.
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
  specified secret." (the service's wire text, which every
  `ResourceNotFoundException` of this service now carries; the model's
  documentation of the exception, "can't find the resource that you asked
  for", is not what a client reads), and a
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
