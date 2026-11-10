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
  a secret whose versions hold no `AWSCURRENT` stage (a stored record from
  before `UpdateSecretVersionStage` refused to remove it; see the next
  entry): the rule checks for that stage, not for any version, and the
  refusal says so, "The secret has no value to rotate: it has no AWSCURRENT
  version." The refusal also applies when the request names a
  `RotationLambdaARN`. That too is the emulator's reading: AWS would accept
  the call and start the rotation asynchronously, and the function would make
  the first version. Here no function runs, so there would be no value to
  seat. A secret that is both without a value and scheduled for
  deletion is refused for the deletion ("You can't perform this operation on
  the secret because it was marked for deletion."), as every other write to
  it is: that rule comes first.
- **AWSCURRENT is moved, never removed.** `UpdateSecretVersionStage` that
  would take `AWSCURRENT` off the version holding it without seating it on
  another is `InvalidParameterException`, "You can only move staging label
  AWSCURRENT to a different secret version. It can't be completely removed."
  Before, the label came off, and the operations then disagreed about the
  secret: `RotateSecret` refused it as having no `AWSCURRENT` version while
  `GetSecretValue` and `BatchGetSecretValue` answered its last value with
  `VersionStages: [AWSCURRENT]`. Of the two repairs, refusing the removal or
  making the reads agree with `RotateSecret`, this is the one the
  documentation supports best, and it removes the cause. What the model says:
  "You can move the AWSCURRENT staging label to this version by including it
  in this call", "Whenever you move AWSCURRENT, Secrets Manager automatically
  moves the label AWSPREVIOUS to the version that AWSCURRENT was removed
  from", its example of deleting a label deletes a custom one and its example
  with `AWSCURRENT` names both `RemoveFromVersionId` and `MoveToVersionId`,
  and `InvalidParameterException` is among the operation's errors. It speaks
  of `AWSCURRENT` only as moved and nowhere says a removal is refused: the
  refusal and its text are the service's wire answer, not a sentence of the
  model. A `MoveToVersionId` that names no version of the secret would take
  the label off and seat it nowhere, so it is refused with the same error;
  that case is this note's reading. Removing any other label, and a removal
  that names a version not holding `AWSCURRENT`, are unchanged.

  So that the operations agree whatever the store holds, the reads were also
  given `RotateSecret`'s rule: a secret none of whose versions holds
  `AWSCURRENT` has no current value. `GetSecretValue` with no `VersionId` or
  `VersionStage` (or with `VersionStage: AWSCURRENT`) is
  `ResourceNotFoundException`, "Secrets Manager can't find the specified
  secret value for staging label: AWSCURRENT", and `BatchGetSecretValue`
  answers that as an `Errors` row in both of its forms. A version is still
  answered by its `VersionId`. No call leaves a secret in that state any
  more; a store written before this change and restored from a snapshot can
  hold one, which is how the tests reach it
  (`TestSecretsManagerRotateNeedsAnAWSCURRENTVersion` now writes the record
  to the store instead of calling `UpdateSecretVersionStage`). Moving
  `AWSCURRENT` still does not move `AWSPREVIOUS` to the version it left; that
  sentence of the documentation is not implemented here.
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
  every version. `IncludeDeprecated` is a `BooleanType`: a value that is not
  a Boolean (the string `"true"`, `"yes"`, `1`) is `SerializationException`,
  the awsJson1_1 codec's answer to a member that is not of its shape's type,
  as a `MaxResults` that is no integer is.
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
    it in the string). The page says nothing of a space inside `${}`;
    CloudFormation answers `${ Q }` with "Template error: variable names in
    Fn::Sub syntax must contain only alphanumeric characters, underscores,
    periods, and colons", and so does this engine, on every operation (the
    service's wording, a reading; round 9 read `" Q "` as an undeclared
    name).
    A `Fn::If` whose condition the parameters settle orders by the branch it
    takes and only that one: `Fn::Equals` of literals and `Ref`s to
    parameters, and `Fn::Not`, `Fn::And`, `Fn::Or` and `Condition` over such
    conditions (Template Reference, condition functions), each condition
    evaluated once per operation however many name it (round 8 evaluated a
    named condition afresh wherever it was named, so a chain of 24
    conditions each naming the last twice took 19 s). A condition they do
    not settle orders by both branches, a reading. A condition "can be
    based on: Input or pseudo parameter values" (User Guide, Conditions
    syntax), and each pseudo parameter the engine answers a `Ref` with
    settles one: `AWS::Region`, `AccountId`, `StackName`, `StackId`,
    `Partition`, `URLSuffix` and `NoValue`, the stack's two only once there
    is a stack, and `AWS::NotificationARNs`, "the list of notification
    Amazon Resource Names (ARNs) for the current stack", the request's
    `NotificationARNs` (round 10 answered its name), and `Fn::FindInMap`, "Mapping values" being a base the page
    names (round 9 settled none, so `!If [East, e, w]` on
    `!Equals [!Ref AWS::Region, us-east-1]` stored `""`). A stack is made
    or updated only with every condition settled: one the engine cannot
    evaluate (a `Fn::Select`) is `ValidationError`, "Template error: the
    condition <name> cannot be evaluated" (a reading). `Fn::FindInMap`
    "returns the value corresponding to keys in a two-level map that's
    declared in the Mappings section", its keys literals, a `Ref` or
    another `Fn::FindInMap` (Template Reference, Fn::FindInMap), in a
    condition, a property and an output (round 10 resolved it nowhere, and
    refused it in a condition); "If omitted, Fn::FindInMap raises an error
    when a key is not found", so a missing map or key is `ValidationError`,
    "Template error: Unable to get mapping for Env::eu-west-1::Stage" (a
    reading), and one the engine cannot evaluate when it makes a stack (a
    `DefaultValue`, which needs the `AWS::LanguageExtensions` transform) is
    refused too. A condition may not
    name a resource ("you can't reference resource logical IDs or their
    attributes in conditions"), "Template format error: Unresolved
    dependencies [P]. Cannot reference resources in the Conditions block of
    the template", and a `Fn::If` or `Condition` may not name a condition
    the template does not declare, "Template error: unresolved condition
    dependency <name>" (both wordings are readings), on every operation.
    "CloudFormation creates the resource only when the condition evaluates
    to true", and "If a resource with a condition isn't created, any
    resources that depend on that resource aren't created either,
    regardless of their own conditions" (Template Reference, Condition
    attribute): such a resource is not made and is not in the stack's
    `Refs`. "CloudFormation creates the output only when the associated
    condition evaluates to true" (User Guide); an output made that names a
    resource not made is `ValidationError`, "Template format error:
    Unresolved resource dependencies [A] in the Outputs block of the
    template" (a reading), before anything is made. Round 9 made every
    resource and output, whatever its condition. A cycle of conditions, one naming itself included, is
    `ValidationError`, "Template format error: Circular dependency between
    conditions: [A, B]" (the wording is a reading), on every operation that
    reads a template; round 9 left each unsettled, so the template passed and
    a `Fn::If` on it stored `""`. A settled `Fn::If` also resolves to its branch,
    and `AWS::NoValue` there "Removes the corresponding resource property"
    (an element of a list too, a `Fn::Join`'s list among them, which the
    Template Reference does not spell out: a reading). It is never a value:
    as part of one, a `Fn::Join`'s separator, a `Fn::Sub`'s string or
    variable, or an output's value, it is `ValidationError` before the
    stack's name is reserved, on `ValidateTemplate` as on `CreateStack`; as
    the whole `Properties` attribute, `!If [C, {...}, !Ref AWS::NoValue]`,
    it leaves the resource no properties (the attribute is no property, so
    this is a reading; round 10 refused it)
    (round 10 found it only after reserving the name, so the refused stack
    stayed `ROLLBACK_COMPLETE` and a retry was `AlreadyExistsException`) (round 9 wrote the sentinel standing for it into the string,
    `"\ufffe,b"`, `"a\ufffeb"`); an unsettled one reaches the service as
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
    case, so `TRUE` protects; it is required, so a request without it is
    `ValidationError` naming it, as the bundle answered, and one that is no
    Boolean, `maybe`, is `ValidationError` too, where round 9 read either as
    false and turned the protection off), as `ContinueUpdateRollback` and
    every other native write already did. Before, the bundle's `UpdateTerminationProtection` read the
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
    and a template that is wrong, by every check its execution would make
    (a parameter or condition without a value, an output naming a resource
    its condition leaves out, round 10 checked the parse alone), leaves the
    change set `FAILED` with the
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
    `UpdateShardCount`, `UpdateSecret`, `UpdateRestApi`, `PutRule`, with the
    targets the previous template gave and the new one does not removed and
    the template's put (a target put on the rule outside its template
    stays), and a bucket's
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
    stack rolls back. One no template can name, a KMS key or a Kinesis
    resource policy, is replaced as any other (round 8 compared its two
    empty names and refused it, "Rename  and update the stack again."). A
    `Ref` to a kept resource answers its id. Before, every
    update made every resource again: a new KMS key and REST API, whose old
    ones the cleanup then deleted, and a failure on any parameter, table, log
    group, function, role or stream, which could only be created. A stream
    is made with the template's `ShardCount`.
  - A failed update's rollback puts back exactly the properties the stack
    had: what the update changed in place is updated back through the same
    update, given the previous template's properties, so a property the
    failed update added is taken away (a target the failed update gave a rule is removed; a secret given a description has an empty one,
    which Secrets Manager no longer answers, as it "only returns fields that
    have a value"); what the update kept is not called. Before, the rollback
    provisioned the previous properties on top of the new ones, and every
    kept resource again.
  - `DeletionPolicy` and `UpdateReplacePolicy` are read as the Template
    Reference gives them (the model's `TemplateConfiguration` names the same
    `DELETE` and `RETAIN`). `Retain` "keeps the resource without deleting the
    resource or its contents": on `DeleteStack`, on an update that drops the
    resource ("it's deleted from CloudFormation's scope"), or whose
    condition it turns off (round 10 read such a resource, still declared,
    as replaced and took its `UpdateReplacePolicy`), and on the rollback
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
    and the other reads; `ListStacks` lists it), and exports nothing, its
    `Outputs` `[]` (round 9 left them null); by its name it does not exist,
    and the name is free. Nothing changes it: `UpdateTerminationProtection`
    by its stack id is "does not exist" (round 9 protected the deleted
    record).
  - A stack's stored template is read as it is stored, its JSON or YAML
    and nothing more: the stack was made from it, and a check added since
    may refuse it. Round 10 parsed it in full wherever it read it
    (`DeleteStack`, an update's comparison, rollback and cleanup,
    `ContinueUpdateRollback`, `GetTemplateSummary`) and dropped the error,
    so a template a newer check refuses read as none, and `DeleteStack`
    deleted a `Retain` resource.
  - An update settles the previous template's conditions again, from the
    record's parameters and the stack's pseudo parameters, before it
    compares: a record written before round 9 holds none in its `Refs`, so
    its `Fn::If` read as written and an unchanged resource was updated (or,
    on a replacing property, replaced). Before, neither attribute
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
  - The cleanup runs after the update is written and can undo nothing of
    it: "In this state, the stack has been updated and is usable, but
    CloudFormation is still deleting the old resources" (User Guide, stack
    status codes, `UPDATE_COMPLETE_CLEANUP_IN_PROGRESS`). A failure of the
    engine's own in the cleanup abandons what it had not yet deleted, as a
    delete that fails does: the stack stays `UPDATE_COMPLETE` with the new
    template, and each such resource is a `DELETE_FAILED` event; the stack's
    reason says the cleanup ended before it deleted every old resource, and
    nothing of the failure. Before, such
    a failure ran the update's rollback over the finished stack: it deleted
    what the update had made and restored the old properties under an
    `UPDATE_COMPLETE` record, and after a replacement neither generation was
    left.
  - An update, and a rollback's restore, leave a resource with exactly its
    template's properties. Each service's update keeps what a call does not
    name, so a property the template no longer gives is cleared through that
    service's own API: a table's stream is disabled and its billing mode and
    table class are the defaults; a function's `Description`, `Environment`,
    `Timeout` ("The default is 3 seconds"), `MemorySize` ("128 MB"),
    `Layers`, `KMSKeyArn`, `DeadLetterConfig`, `TracingConfig` and
    `EphemeralStorage` are the values a function made without them has; a
    bucket's tags, public access block, ownership controls and stored
    configurations are deleted. A bucket's versioning is suspended, since
    "Once you version-enable a bucket, it can never return to an unversioned
    state. You can, however, suspend versioning on that bucket". Only what
    the template the resource is updated from gave is cleared: an update
    clears what the previous template gave and the new one does not, and a
    rollback what the failed update gave and the previous template does not.
    What was set on a resource outside its template stays, through an
    update of another property, an update that changes nothing and a
    rollback; a resource declared with no `Properties` was given nothing.
    The rule is one for every type: a secret's and a key's description are
    cleared only when the previous template gave one, a stream goes back to
    one shard only when the previous template gave a `ShardCount`, and a
    rule's targets are removed only when the previous template gave them.
    A resource that is `UPDATE_FAILED` records the names of the properties
    the failed update gave another value than its template's (names, never
    values; none is recorded as an empty list), and the ids of the targets
    it gave a rule, so `ContinueUpdateRollback` clears, and removes
    targets, by the same rule. A record written before the names were kept
    has none: for it alone `ContinueUpdateRollback` clears every such
    property the template lacks, removes every target the template does not
    give, and suspends a bucket's versioning only where it is on. A resource
    recorded before resources recorded the name they were made under is
    put back under the name its template gives it, or the generated one.
    A secret's value follows the rule too: `UpdateSecret` makes a new
    version only "If you include SecretString or SecretBinary", so the
    engine sends the value only when the template's `SecretString` or
    `GenerateSecretString` differs from what the previous template gave. An
    update of a secret's tags or description, an update that changes
    nothing and a rollback of an update that did not change the value all
    leave a value rotated outside the template as it is; before, each sent
    the template's value again, and made a generated password anew. Four
    updates replace what is there with the template's, because the template
    must give it and the call is a whole replacement: a parameter's value
    and type, a role's trust policy, a stream's resource policy, and a rule
    ("If you omit arguments in PutRule, the old values for those arguments
    are not kept"), so a rule's description or state set outside the
    template does not outlive an update of the rule, in the service as
    here. A table's `DeletionProtectionEnabled` is not sent at
    all: the table service here keeps it false.
    One property cannot be cleared, in the service as here: a secret's
    value. Secrets Manager has no call that takes a value away (a new value
    is a new version, and `AWSCURRENT` is moved, never removed), so a secret
    whose template gave no `SecretString` keeps the value a failed update
    gave it. Before, an update sent only the properties the template gave,
    and a failed update's additions outlived its rollback.
  - A logical id keeps its type. An update that gives one another type is
    `ValidationError`, "Update of resource type is not permitted. The new
    template modifies resource type of the following resources: [X]" (the
    service's wording as users report it; the guides do not print it, so a
    reading), and changes nothing. Before, the update made the new type and
    its cleanup deleted the old resource as a replaced one, a
    `DeletionPolicy: Retain` one included.
  - A resource is told by its type and its physical id together, wherever
    the engine asks whether a stack holds it: a bucket and a parameter may
    share a name. Before, the cleanup matched the id alone, and a dropped
    bucket whose name a kept parameter had was left behind, listed by no
    stack.
  - `ListStacks` reads `StackStatusFilter`: it "Returns the summary
    information for stacks whose status matches the specified
    StackStatusFilter", and "If no StackStatusFilter is specified, summary
    information for all stacks is returned (including existing stacks and
    stacks that have been deleted)". A deleted stack is listed for as long
    as its record is kept ("Deleted stacks are retained and viewable for 90
    days"; here, until its name is used again, which replaces the record:
    the deleted stack then answers by its id no more). Before, the filter
    was not read.
  - What an operation that could not settle its stack leaves for the next
    one is kept in the process's memory, at most 1024 of them. After the
    process restarts, or past that bound, the next operation on a stack an
    update left in progress settles it `UPDATE_ROLLBACK_FAILED` with
    nothing marked: it does not know what the update had changed or made,
    and `ContinueUpdateRollback` then answers `UPDATE_ROLLBACK_COMPLETE`
    over the resources as they are.
  - `ContinueUpdateRollback` reserves its stack as every operation does:
    the stack is `UPDATE_ROLLBACK_IN_PROGRESS` while it runs, a
    `DeleteStack`, an `UpdateStack` or a second `ContinueUpdateRollback`
    during it is refused, and one that ends without settling the stack is
    taken over by the next operation, which settles it
    `UPDATE_ROLLBACK_FAILED`. Before, it took no reservation.
  - `DeletionPolicy: Retain` holds on every rollback of the update that made
    the resource, one taken over by a later operation included: the resource
    leaves the stack, `DELETE_SKIPPED` ("The DELETE_SKIPPED status applies
    to resources with a deletion policy attribute of retain"), and
    `RetainExceptOnCreate`'s is deleted. Before, a rollback that was taken
    over listed both for `ContinueUpdateRollback`, which deleted both.
  - A rollback puts back every resource the update changed in place, one
    whose change was not finished included (a failure of the engine's own
    part way through a bucket's update leaves its tags changed and its
    stored configuration not: both are put back). A rollback that ends
    before it has put everything back leaves each
    resource the update changed in place and it had not restored
    `UPDATE_FAILED` on the record the next operation settles, so
    `ContinueUpdateRollback` restores it; what it had restored is not
    marked. Before, such a resource was left
    as it was listed, `ContinueUpdateRollback` answered
    `UPDATE_ROLLBACK_COMPLETE`, and the resource kept the failed update's
    values under the previous template.
  - A change set whose template gives a logical id another type is `FAILED`
    when it is made, with the refusal as its `StatusReason`, as every other
    check its execution would make. Before, it was `AVAILABLE` and refused
    only when it was executed.
  - A rollback that cannot delete what the failed update made leaves the
    stack `UPDATE_ROLLBACK_FAILED` with the resource listed, so that
    `ContinueUpdateRollback` and `DeleteStack` retry it and no other stack
    takes its name. The User Guide puts that delete after the rollback
    (`UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS`: "the stack has been
    rolled back to its previous working state and is usable, but
    CloudFormation is still deleting any new resources it created during
    the stack update") and does not say what a delete that fails there
    leaves; this engine keeps the resource the stack's rather than abandon
    it. This is unchanged.
  - A deleted stack changes no more: `DeleteStack` by the stack id of a
    deleted stack answers as a delete does and writes nothing. A stack that
    is deleted cleanly has no `StackStatusReason`, whatever an earlier
    operation failed with; `ROLLBACK_COMPLETE` and
    `UPDATE_ROLLBACK_COMPLETE` keep the reason the operation rolled back
    for, a reading of "Success/failure message associated with the stack
    status". Before, the delete by id reserved and settled the record
    again, and a deleted stack kept an earlier failure's reason.
  - A table's `KeySchema` and `LocalSecondaryIndexes` are set when the table
    is made: `UpdateTable` takes neither, so a change to one replaces the
    table. The Template Reference prints "Update requires: Some
    interruptions" and "No interruption" for them, which no DynamoDB call
    can carry out, so this is a reading. A bucket's `ObjectLockEnabled` is a
    replacement too. Before, the update called `UpdateTable` without them
    and reported the table updated.
  - The previous template is read with what its own stack read:
    `AWS::NotificationARNs` in it answers the ARNs the stack had, so an
    update that changes the ARNs changes a resource built on them, and a
    rollback puts back the old ones. Before, the previous properties were
    read with the request's ARNs.
  - EventBridge's `DeleteRule` deletes the rule's targets record, empty by
    then, with the rule.
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
    can then be updated, a skipped resource kept as any other the stack
    holds (round 8 took only a resource with no status for one the stack
    holds, so the next update made a skipped one again and failed, the
    service's error for a name taken); a retry that fails leaves it
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
    YAML, is its text `"16"`; before, it became `""`. "For parameters
    without default values, users must specify a key name value at stack
    creation", or it is "Parameters: [KeyName] must have values" (User
    Guide, Parameters): `CreateStack` and `UpdateStack` refuse such a
    template in those words before anything is reserved (round 10 left the
    parameter out). Each
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
  `GetSecretValue` answers for that secret alone, with one documented
  exception: a full ARN in `SecretIdList`. `BatchGetSecretValue` reads each id
  as the secret's name and does not strip an ARN's prefix, as every other
  operation does, so the row for an ARN is `ResourceNotFoundException`,
  "Secrets Manager can't find the specified secret.", whatever
  `GetSecretValue` answers for that ARN (the value, or another error). That
  is the hand-written pack's behaviour, kept and recorded among the bundle's
  quirks. For a name: a secret that does not
  exist is `ResourceNotFoundException`, "Secrets Manager can't find the
  specified secret." (the service's wire text, which every
  `ResourceNotFoundException` of this service now carries; the model's
  documentation of the exception, "can't find the resource that you asked
  for", is not what a client reads), and a
  secret with no value "Secrets Manager can't find the specified secret value
  for staging label: AWSCURRENT". `GetSecretValue` of a scheduled secret
  answers the same message as the other refused calls. Before, a not-found
  row had no `Message` and the single call's was empty. `GetSecretValue` of a
  secret with no value names what it was asked for: a `VersionStage` is "for
  staging label: AWSPREVIOUS" (the label asked for), and a `VersionId` is
  "Secrets Manager can't find the specified secret value for VersionId: abc".
  It said "for staging label: AWSCURRENT" whatever was asked.
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
