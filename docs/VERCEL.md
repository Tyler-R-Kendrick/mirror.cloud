# Vercel

`vercel.api` is an emulation of the Vercel REST API (`api.vercel.com`) good
enough to deploy through: a project, its variables, a file upload, a deployment
that builds on the clock, an alias, a domain, an Edge Config. Vercel's own CLI and
its SDK are the clients it is written for.

What that claim rests on, stated once so the rest does not have to:

- **Nothing here has been compared with `api.vercel.com`.** There is no live
  differential. The behaviour is the vendored OpenAPI document's, plus what
  Vercel's own clients and its open-source emulator (vercel-labs/emulate, the
  "oracle") are known to do where the document is silent.
- **`spec-validated`** (the Status column below) means the conformance run drives
  the operation and holds every answer to the schema the vendored document
  declares for it. A client generated from the document, the SDK among them,
  validates responses the same way, so it can read what the emulator sends. It
  does not mean Vercel would have sent the same thing.
- **`authored`** means the operation is not in the vendored document, or the
  document is wrong about it (see [Where the document is wrong](#where-the-document-is-wrong)),
  and the answer is held to a schema written here (`specs/vercel/api-extra.json`).
  Every cell of the model carries `authored` provenance as well: transcribed from
  the oracle or written from the document, never observed.

## Start it

```sh
mirror up vercel.api                 # :4566, any credential
mirror up vercel.api aws.s3          # beside the services a pipeline also uses
mirror up vercel.api --seed ci-42 --persist ./.mirror-state
```

A request is Vercel's when it arrives with `Host: api.vercel.com` or, failing a
host, when its path names one of Vercel's operations (checked with every service
enabled), so a client pointed at `http://127.0.0.1:4566` needs no host rewriting. The emulator listens on loopback with **no authentication**; do
not expose it.

| Setting | Effect |
|---|---|
| `--seed` / `MIRROR_SEED` | Every generated id, URL and token is a function of the seed and the order of requests. The same seed and the same requests give the same bodies. |
| `--persist DIR` / `MIRROR_PERSIST` | State is written to `DIR/state.tar` on exit and read at start. The identifier stream resumes where it stood, so what is created after a restart does not reuse an id. |
| `MIRROR_CLOCK=controllable` | The clock moves only when told: `POST /_mirror/clock/advance {"duration":"3s"}`. Builds settle on it. |
| `MIRROR_VERCEL_FAULTS=1` | Turns on the control routes under `/_mirror/vercel/` (see [Fault injection](#fault-injection)). Off, they do not exist. |
| `MIRROR_VERCEL_STRICT_AUTH=1` | Refuses requests with no usable credential, as Vercel does. Off, any credential, or none, works. |

## Point a client at it

```sh
export MIRROR_URL=http://127.0.0.1:4566
curl -s $MIRROR_URL/v2/user -H 'Authorization: Bearer anything'

vercel --api $MIRROR_URL whoami            # the CLI; the token goes in VERCEL_TOKEN
```

```js
import { Vercel } from "@vercel/sdk";
const vercel = new Vercel({ bearerToken: "anything", serverURL: process.env.MIRROR_URL });
```

`scripts/vercel-acceptance.sh` is the whole deploy path in curl, and the thing to
read to see what a client sends: a project created at `v9` and at `v11`, a
variable, `POST /v2/files` with the SHA-1 in `x-vercel-digest`, `POST /v13/deployments`
naming that file, a poll to READY, an alias, the deployment read back by id and
by URL, and the project deleted. `TestTheAcceptanceScriptDeploysAndItsAnswersAreTheDocuments`
runs it against a booted emulator and checks each answer against the document.

The emulator takes any token. For a real deploy, hand the token to the command
through a secrets broker, not through an env file.

### Scope: users and teams

`/v2/user` is one caller. A team is made with `POST /v1/teams` (or `/v2/teams`) and
acted for with `?teamId=` or `?slug=` on any request; the team's projects,
deployments, domains, Edge Configs and aliases are its own, and a name one team
holds is free in the next. An unknown `?slug=` is a 403, as it is on Vercel.

## What a deployment does

A deployment is created `QUEUED`, becomes `BUILDING` one second later and `READY`
one second after that, on the emulator's clock. Nothing builds: the files are
recorded, and the deployment's URL is not served. Upload files first, by the SHA-1
of their bytes (`POST /v2/files`), or send them inline (`{"file": "index.html",
"data": "..."}`); naming a file that was neither uploaded nor inlined is the 400
`missing_files` with the list of what is missing, which is how the CLI knows what
to upload. A deployment for a project name nobody has made creates the project;
Vercel answers 404 there, and this is the one place the emulator is kinder.

Aliases need a `READY` deployment. Assigning a hostname that already names another
deployment moves it. A production deployment is aliased to `<project>.vercel.app`
once it is ready.

## Versions

Vercel versions each route, keeps answering the old versions, and its clients were
written against whichever was current. The vendored document names each route once,
at its newest version, so `specs/vercel/clients.json` lists the other addresses an
operation also answers at (the table's third column): project creation at `/v9`,
`/v10` and `/v11`, project variables at `/v8` through `/v10`, and so on. Every
address of an operation reaches the same store, so a project made with the CLI's
`v9` is the one an SDK lists at `v10`. A version nobody wrote down is not served: sent
with `Host: api.vercel.com` it is a 501 `MirrorNotImplemented`, never a silent success
(without the host, a path no service claims falls to S3's routes, whose 501 it gets). The Edge Config routes answer at `/v1/edge-config`, which the API
serves, and at `/v1/global-config`, which the document spells.

## Errors

Every error is Vercel's envelope, `{"error": {"code": "...", "message": "..."}}`
(OAuth's token route answers RFC 6749's flat form instead). Statuses follow the
document: 400 for a body or query that is wrong, 404 for what is not there, 409 for
what is already there (a project name, a team slug, a hostname held by another
project, an existing variable), 429 under a rate limit. The codes are Vercel's where
the CLI keys on them (`missing_files`, `ENV_CONFLICT` with `envVarKey` and `target`,
`forbidden` with `invalidToken`, `rate_limited`) and this emulator's where the
document names only the status; the quirks in `behavior/vercel/api/service.yaml`
say which.

## Fault injection

With `MIRROR_VERCEL_FAULTS=1` the control routes exist. They take no credential and
are never rate limited, and they act on the team they name like any other request.

```sh
# The next deployment ends in ERROR a second after it starts building.
curl -X POST $MIRROR_URL/_mirror/vercel/deployments/fail-next \
  -d '{"count": 1, "readyState": "ERROR", "errorCode": "BUILD_FAILED", "errorMessage": "npm run build exited 1"}'
curl -X DELETE $MIRROR_URL/_mirror/vercel/deployments/fail-next          # disarm

# Move a deployment to a state now (BUILDING, READY, ERROR, CANCELED).
curl -X POST $MIRROR_URL/_mirror/vercel/deployments/dpl_.../state -d '{"readyState": "CANCELED"}'

# The next three requests to vercel.api answer 429 with Retry-After: 2.
curl -X POST $MIRROR_URL/_mirror/vercel/rate-limit -d '{"requests": 3, "retryAfter": 2}'
curl -X DELETE $MIRROR_URL/_mirror/vercel/rate-limit                      # disarm
```

With `MIRROR_VERCEL_STRICT_AUTH=1`, a request with no `Authorization: Bearer` header
is a 403 `forbidden` ("Not authenticated"); a token that starts with `invalid` is a
403 `forbidden` carrying `invalidToken: true`, which is what the CLI keys on to ask
you to log in again.

## Persistence and determinism

`--seed` fixes the ids. The clock fixes the instants when it is controllable, and
otherwise the instants are the wall clock's. A transcript of the deploy path is
byte-identical across two runs of one seed on the controllable clock
(`TestVercelIsDeterministic`), and a restart from `--persist` keeps the project, its
variables, the deployments and the Edge Configs while later requests draw new ids
(`TestVercelStateSurvivesARestart`).

## Real clients

`TestVercelsOwnClientsDeploy` runs Vercel's CLI and SDK against a booted emulator.
It needs a directory with their packages installed:

```sh
mkdir ~/vercel-clients && cd ~/vercel-clients
# install `vercel` and `@vercel/sdk` here with your package manager
MIRROR_VERCEL_CLIENT_DIR=~/vercel-clients go test ./test/behavior/vercel -run TestVercelsOwnClientsDeploy -v
```

Without the variable it skips and says so. The journeys (`test/vercel-clients/`)
were written from the clients' documented interfaces and **have not been run**:
no toolchain was installed where they were written. Expect the first run to find
something; a failure there is a finding about the emulator or about the script.

## Conformance

```sh
go test ./test/behavior/vercel -run TestVercelResponsesConformToTheDocument -v
```

drives every operation in the table through the running emulator, at every address
it answers at, and checks each answer against the schema the vendored document (or
the supplement) declares for that operation and status. A served operation nothing
drives, or an address no step calls, fails the run, so the table cannot outgrow the
test. Two operations are driven elsewhere and say why in `confUncovered`: the OAuth
login page (it answers HTML) and its callback (a 302 with no body).

The checker is small and written for this document (`oas_test.go`): `type`,
`properties`, `required`, `enum`, `nullable`, `items`, `additionalProperties`,
`oneOf` (at least one variant, since several of the document's are not exclusive),
`anyOf`, `allOf`, bounds, `pattern`, and the `date-time` and `uri` formats. It
enforces `additionalProperties: false`. A JSON Schema library would be a new
dependency for a document this plain. `TestOASCheckerRejectsWhatItShould` pins what
it refuses.

An error status the document does not list for an operation (it omits the 404 on
`getProject`, for one) is not held to a schema, only to the envelope; the run logs
which.

### Where the document is wrong

- Two Edge Config listings (`getEdgeConfigs`, `getEdgeConfigTokens`) are described as
  one object under a sentence that says they list. They answer arrays here, and the
  supplement says so.
- `updateRollingReleaseConfig` declares no request body. The members are read from
  the body anyway.
- The artifact download is filed under `application/json` as a binary string. The
  answer is the bytes.
- A project check's `source` and a check run's `source` offer variants that do not
  include the `vercel` kind the `sourceKind` enum does.

## Supported surface

Generated from the model by `TestVercelDocListsTheSurfaceTheModelServes`;
`UPDATE_VERCEL_DOC=1 go test ./test/behavior/vercel -run TestVercelDocListsTheSurface`
rewrites it. `docs/SUPPORT.md` is generated separately, by `mirror support-matrix`.

<!-- surface:begin (generated by TestVercelDocListsTheSurfaceTheModelServes; UPDATE_VERCEL_DOC=1 rewrites it) -->

| Operation | Method and address | Also answers at | Status |
|---|---|---|---|
| `AddProjectDomain` | POST `/v10/projects/{idOrName}/domains` | `/v8/projects/{idOrName}/domains`, `/v9/projects/{idOrName}/domains` | spec-validated |
| `ApproveRollingReleaseStage` | POST `/v1/projects/{idOrName}/rolling-release/approve-stage` | - | spec-validated |
| `ArtifactExists` | HEAD `/v8/artifacts/{hash}` | - | spec-validated |
| `ArtifactQuery` | POST `/v8/artifacts` | - | spec-validated |
| `AssignAlias` | POST `/v2/deployments/{id}/aliases` | `/now/deployments/{id}/aliases`, `/v3/deployments/{id}/aliases` | spec-validated |
| `BatchRemoveProjectEnv` | DELETE `/v1/projects/{idOrName}/env` | - | spec-validated |
| `CancelDeployment` | PATCH `/v12/deployments/{id}/cancel` | - | spec-validated |
| `CompleteRollingRelease` | POST `/v1/projects/{idOrName}/rolling-release/complete` | - | spec-validated |
| `CreateApiKey` | POST `/v1/api-keys` | - | authored (held to the supplement's schema) |
| `CreateCheck` | POST `/v1/deployments/{deploymentId}/checks` | - | spec-validated |
| `CreateCustomEnvironment` | POST `/v9/projects/{idOrName}/custom-environments` | - | spec-validated |
| `CreateDeployment` | POST `/v13/deployments` | - | spec-validated |
| `CreateDeploymentCheckRun` | POST `/v2/deployments/{deploymentId}/check-runs` | - | spec-validated |
| `CreateEdgeConfig` | POST `/v1/global-config` | `/v1/edge-config` | spec-validated |
| `CreateEdgeConfigToken` | POST `/v1/global-config/{edgeConfigId}/token` | `/v1/edge-config/{edgeConfigId}/token` | spec-validated |
| `CreateOrTransferDomain` | POST `/v7/domains` | `/v4/domains` | spec-validated |
| `CreateProject` | POST `/v11/projects` | `/v1/projects`, `/v10/projects`, `/v9/projects` | spec-validated |
| `CreateProjectCheck` | POST `/v2/projects/{projectIdOrName}/checks` | - | spec-validated |
| `CreateProjectEnv` | POST `/v10/projects/{idOrName}/env` | `/v8/projects/{idOrName}/env`, `/v9/projects/{idOrName}/env` | spec-validated |
| `CreateTeam` | POST `/v2/teams` | `/v1/teams` | authored (held to the supplement's schema) |
| `DeleteAlias` | DELETE `/v2/aliases/{aliasId}` | `/v4/aliases/{aliasId}` | spec-validated |
| `DeleteAllArtifacts` | DELETE `/v8/artifacts` | - | spec-validated |
| `DeleteApiKey` | DELETE `/v1/api-keys/{keyId}` | - | authored (held to the supplement's schema) |
| `DeleteDeployment` | DELETE `/v13/deployments/{id}` | - | spec-validated |
| `DeleteDomain` | DELETE `/v6/domains/{domain}` | `/v4/domains/{domain}`, `/v5/domains/{domain}` | spec-validated |
| `DeleteEdgeConfig` | DELETE `/v1/global-config/{edgeConfigId}` | `/v1/edge-config/{edgeConfigId}` | spec-validated |
| `DeleteEdgeConfigSchema` | DELETE `/v1/global-config/{edgeConfigId}/schema` | `/v1/edge-config/{edgeConfigId}/schema` | spec-validated |
| `DeleteEdgeConfigTokens` | DELETE `/v1/global-config/{edgeConfigId}/tokens` | `/v1/edge-config/{edgeConfigId}/tokens` | spec-validated |
| `DeleteProject` | DELETE `/v9/projects/{idOrName}` | `/projects/{idOrName}` | spec-validated |
| `DeleteProjectCheck` | DELETE `/v2/projects/{projectIdOrName}/checks/{checkId}` | - | spec-validated |
| `DeleteRollingReleaseConfig` | DELETE `/v1/projects/{idOrName}/rolling-release/config` | - | spec-validated |
| `DownloadArtifact` | GET `/v8/artifacts/{hash}` | - | spec-validated; the body is bytes, so only the status is held |
| `EditProjectEnv` | PATCH `/v9/projects/{idOrName}/env/{id}` | `/v10/projects/{idOrName}/env/{id}`, `/v8/projects/{idOrName}/env/{id}` | spec-validated |
| `FilterProjectEnvs` | GET `/v10/projects/{idOrName}/env` | `/v8/projects/{idOrName}/env`, `/v9/projects/{idOrName}/env` | spec-validated |
| `GetAlias` | GET `/v4/aliases/{idOrAlias}` | `/v2/aliases/{idOrAlias}`, `/v3/aliases/{idOrAlias}` | spec-validated |
| `GetAllChecks` | GET `/v1/deployments/{deploymentId}/checks` | - | spec-validated |
| `GetAuthUser` | GET `/v2/user` | - | spec-validated |
| `GetCheck` | GET `/v1/deployments/{deploymentId}/checks/{checkId}` | - | spec-validated |
| `GetCustomEnvironment` | GET `/v9/projects/{idOrName}/custom-environments/{environmentSlugOrId}` | - | spec-validated |
| `GetDeployment` | GET `/v13/deployments/{idOrUrl}` | - | spec-validated |
| `GetDeploymentBuilds` | GET `/v11/deployments/{deploymentId}/builds` | - | authored (held to the supplement's schema) |
| `GetDeploymentCheckRun` | GET `/v2/deployments/{deploymentId}/check-runs/{checkRunId}` | - | spec-validated |
| `GetDeploymentEvents` | GET `/v3/deployments/{idOrUrl}/events` | `/v3/now/deployments/{idOrUrl}/events` | spec-validated |
| `GetDeploymentFileContents` | GET `/v8/deployments/{id}/files/{fileId}` | - | spec-validated |
| `GetDeployments` | GET `/v7/deployments` | - | spec-validated |
| `GetDeploymentsV6` | GET `/v6/deployments` | - | authored (held to the supplement's schema) |
| `GetDomain` | GET `/v5/domains/{domain}` | `/v4/domains/{domain}`, `/v6/domains/{domain}` | spec-validated |
| `GetDomainConfig` | GET `/v6/domains/{domain}/config` | - | spec-validated |
| `GetDomainVerificationRecord` | GET `/v9/domains/{domain}/verification` | - | spec-validated |
| `GetDomains` | GET `/v5/domains` | `/v4/domains`, `/v6/domains` | spec-validated |
| `GetEdgeConfig` | GET `/v1/global-config/{edgeConfigId}` | `/v1/edge-config/{edgeConfigId}` | spec-validated |
| `GetEdgeConfigItem` | GET `/v1/global-config/{edgeConfigId}/item/{edgeConfigItemKey}` | `/v1/edge-config/{edgeConfigId}/item/{edgeConfigItemKey}` | spec-validated |
| `GetEdgeConfigItems` | GET `/v1/global-config/{edgeConfigId}/items` | `/v1/edge-config/{edgeConfigId}/items` | spec-validated |
| `GetEdgeConfigSchema` | GET `/v1/global-config/{edgeConfigId}/schema` | `/v1/edge-config/{edgeConfigId}/schema` | spec-validated |
| `GetEdgeConfigToken` | GET `/v1/global-config/{edgeConfigId}/token/{token}` | `/v1/edge-config/{edgeConfigId}/token/{token}` | spec-validated |
| `GetEdgeConfigTokens` | GET `/v1/global-config/{edgeConfigId}/tokens` | `/v1/edge-config/{edgeConfigId}/tokens` | authored (held to the supplement's schema) |
| `GetEdgeConfigs` | GET `/v1/global-config` | `/v1/edge-config` | authored (held to the supplement's schema) |
| `GetProject` | GET `/v9/projects/{idOrName}` | - | spec-validated |
| `GetProjectCheck` | GET `/v2/projects/{projectIdOrName}/checks/{checkId}` | - | spec-validated |
| `GetProjectDomain` | GET `/v9/projects/{idOrName}/domains/{domain}` | `/v10/projects/{idOrName}/domains/{domain}`, `/v8/projects/{idOrName}/domains/{domain}` | spec-validated |
| `GetProjectDomains` | GET `/v9/projects/{idOrName}/domains` | `/v10/projects/{idOrName}/domains`, `/v8/projects/{idOrName}/domains` | spec-validated |
| `GetProjectEnv` | GET `/v1/projects/{idOrName}/env/{id}` | `/v10/projects/{idOrName}/env/{id}`, `/v8/projects/{idOrName}/env/{id}`, `/v9/projects/{idOrName}/env/{id}` | spec-validated |
| `GetProjects` | GET `/v10/projects` | `/v9/projects` | spec-validated |
| `GetProjectsByIdOrNameCustomEnvironments` | GET `/v9/projects/{idOrName}/custom-environments` | `/projects/{idOrName}/custom-environments` | spec-validated |
| `GetRegistration` | GET `/registration` | - | authored (held to the supplement's schema) |
| `GetRollingRelease` | GET `/v1/projects/{idOrName}/rolling-release` | - | spec-validated |
| `GetRollingReleaseBillingStatus` | GET `/v1/projects/{idOrName}/rolling-release/billing` | - | spec-validated |
| `GetRollingReleaseConfig` | GET `/v1/projects/{idOrName}/rolling-release/config` | - | spec-validated |
| `GetRuntimeLogs` | GET `/v1/projects/{projectId}/deployments/{deploymentId}/runtime-logs` | - | spec-validated |
| `GetTeam` | GET `/v2/teams/{teamId}` | `/teams/{teamId}` | spec-validated |
| `GetTeamMembers` | GET `/v3/teams/{teamId}/members` | - | spec-validated |
| `GetTeams` | GET `/v2/teams` | `/v1/teams` | spec-validated |
| `InviteUserToTeam` | POST `/v2/teams/{teamId}/members` | - | spec-validated |
| `ListAliases` | GET `/v4/aliases` | `/v2/aliases`, `/v3/aliases`, `/v3/now/aliases` | spec-validated |
| `ListApiKeys` | GET `/v1/api-keys` | - | authored (held to the supplement's schema) |
| `ListCheckRuns` | GET `/v2/projects/{projectIdOrName}/checks/{checkId}/runs` | - | spec-validated |
| `ListDeploymentAliases` | GET `/v2/deployments/{id}/aliases` | `/v3/deployments/{id}/aliases` | spec-validated |
| `ListDeploymentCheckRuns` | GET `/v2/deployments/{deploymentId}/check-runs` | - | spec-validated |
| `ListDeploymentFiles` | GET `/v6/deployments/{id}/files` | - | spec-validated |
| `ListProjectChecks` | GET `/v2/projects/{projectIdOrName}/checks` | - | spec-validated |
| `ListPromoteAliases` | GET `/v1/projects/{projectId}/promote/aliases` | - | spec-validated |
| `ListTeamMembers` | GET `/v2/teams/{teamId}/members` | - | authored (held to the supplement's schema) |
| `MirrorArmDeploymentFailure` | POST `/_mirror/vercel/deployments/fail-next` | - | authored (mirror's own control route, off unless started with MIRROR_VERCEL_FAULTS) |
| `MirrorDisarmDeploymentFailure` | DELETE `/_mirror/vercel/deployments/fail-next` | - | authored (mirror's own control route, off unless started with MIRROR_VERCEL_FAULTS) |
| `MirrorSetDeploymentState` | POST `/_mirror/vercel/deployments/{idOrUrl}/state` | - | authored (mirror's own control route, off unless started with MIRROR_VERCEL_FAULTS) |
| `OauthAuthorize` | GET `/oauth/authorize` | - | authored (driven end to end; answers the login page as HTML, which no JSON schema describes) |
| `OauthAuthorizeCallback` | POST `/oauth/authorize/callback` | - | authored (driven end to end; answers a 302 to the redirect URI carrying the code, with no body) |
| `OauthToken` | POST `/login/oauth/token` | - | authored (held to the supplement's schema) |
| `OauthUserinfo` | GET `/login/oauth/userinfo` | - | authored (held to the supplement's schema) |
| `PatchDomain` | PATCH `/v3/domains/{domain}` | - | spec-validated |
| `PatchEdgeConfigItems` | PATCH `/v1/global-config/{edgeConfigId}/items` | `/v1/edge-config/{edgeConfigId}/items` | spec-validated |
| `PatchEdgeConfigSchema` | POST `/v1/global-config/{edgeConfigId}/schema` | `/v1/edge-config/{edgeConfigId}/schema` | spec-validated |
| `PatchTeam` | PATCH `/v2/teams/{teamId}` | - | spec-validated |
| `PatchUrlProtectionBypass` | PATCH `/aliases/{id}/protection-bypass` | - | spec-validated |
| `RecordEvents` | POST `/v8/artifacts/events` | - | spec-validated |
| `RemoveCustomEnvironment` | DELETE `/v9/projects/{idOrName}/custom-environments/{environmentSlugOrId}` | - | spec-validated |
| `RemoveProjectDomain` | DELETE `/v9/projects/{idOrName}/domains/{domain}` | `/v10/projects/{idOrName}/domains/{domain}`, `/v8/projects/{idOrName}/domains/{domain}` | spec-validated |
| `RemoveProjectEnv` | DELETE `/v9/projects/{idOrName}/env/{id}` | `/v10/projects/{idOrName}/env/{id}`, `/v8/projects/{idOrName}/env/{id}` | spec-validated |
| `RequestPromote` | POST `/v10/projects/{projectId}/promote/{deploymentId}` | - | spec-validated |
| `RequestRollback` | POST `/v1/projects/{projectId}/rollback/{deploymentId}` | - | spec-validated |
| `RerequestCheck` | POST `/v1/deployments/{deploymentId}/checks/{checkId}/rerequest` | - | spec-validated |
| `StartRollingRelease` | POST `/v1/projects/{idOrName}/rolling-release/start` | - | spec-validated |
| `Status` | GET `/v8/artifacts/status` | - | spec-validated |
| `UpdateAuthUser` | PATCH `/v2/user` | - | authored (held to the supplement's schema) |
| `UpdateCheck` | PATCH `/v1/deployments/{deploymentId}/checks/{checkId}` | - | spec-validated |
| `UpdateCustomEnvironment` | PATCH `/v9/projects/{idOrName}/custom-environments/{environmentSlugOrId}` | - | spec-validated |
| `UpdateDeploymentCheckRun` | PATCH `/v2/deployments/{deploymentId}/check-runs/{checkRunId}` | - | spec-validated |
| `UpdateEdgeConfig` | PUT `/v1/global-config/{edgeConfigId}` | `/v1/edge-config/{edgeConfigId}` | spec-validated |
| `UpdateProject` | PATCH `/v9/projects/{idOrName}` | - | spec-validated |
| `UpdateProjectCheck` | PATCH `/v2/projects/{projectIdOrName}/checks/{checkId}` | - | spec-validated |
| `UpdateProjectDomain` | PATCH `/v9/projects/{idOrName}/domains/{domain}` | `/v10/projects/{idOrName}/domains/{domain}`, `/v8/projects/{idOrName}/domains/{domain}` | spec-validated |
| `UpdateProjectProtectionBypass` | PATCH `/v1/projects/{idOrName}/protection-bypass` | - | spec-validated |
| `UpdateProjectsByProjectIdRollbackByDeploymentIdUpdateDescription` | PATCH `/v1/projects/{projectId}/rollback/{deploymentId}/update-description` | - | spec-validated |
| `UpdateRollingReleaseConfig` | PATCH `/v1/projects/{idOrName}/rolling-release/config` | - | spec-validated |
| `UploadArtifact` | PUT `/v8/artifacts/{hash}` | - | spec-validated |
| `UploadFile` | POST `/v2/files` | - | spec-validated |
| `VerifyProjectDomain` | POST `/v9/projects/{idOrName}/domains/{domain}/verify` | `/v10/projects/{idOrName}/domains/{domain}/verify`, `/v8/projects/{idOrName}/domains/{domain}/verify` | spec-validated |

<!-- surface:end -->

## Known gaps

What a person deploying through this will run into, roughly in the order they will.

- **No build, no serving.** A deployment's files are recorded and its URL answers
  nothing. `GetDeploymentFileContents` is an empty object; a file's bytes are not
  kept (the remote cache's are).
- **Build logs do not stream.** `GET /v3/deployments/{id}/events` answers the events
  the emulator records (created, building, ready, canceled) as a finite list;
  `follow=1` and the other query members are accepted and read by nothing, and
  there is no build output to follow.
- **About 300 of the document's 417 operations are not served**: the marketplace and
  integrations, log drains and webhooks, sandboxes, storage, firewall, feature
  flags, certificates, DNS records, domain purchase and transfer (the registrar),
  access groups, shared variables, and the rest. They answer 501. `vercel env pull`
  uses an address the document does not list and is not served.
- **A domain is never resolved.** Verifying a project domain takes the TXT challenge
  as met; there is no DNS to ask. An alias to a hostname the account does not hold is
  accepted, where Vercel answers 404.
- **Edge Config:** the control plane only. Its data plane
  (`edge-config.vercel.com`, what an application reads through with a token) is
  another host and another product. Backups are not served. A config's schema is
  stored and returned, never enforced against its items. The digest is a hash of the
  config's id and a revision, not of its items.
- **Rolling releases** are a state machine a client steps through; no traffic is
  split. Billing is "unlimited".
- **Teams** are created, read, renamed and given members by email; there is no
  invitation to accept, no roles beyond the stored string, no billing.
- **Pagination** cursors are millisecond timestamps, as Vercel's are. Items created
  within one millisecond of each other can be missed between pages; on a
  controllable clock that is every item until the clock moves.
- **The OAuth flow** is the oracle's local login (a page, a callback, a token
  exchange, userinfo), not Vercel's.
- **Rate limiting** is the armed 429s and nothing else; there is no accounting.
