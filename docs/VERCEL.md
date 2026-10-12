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
- **Client-observed** is what a Vercel client was seen to send, or to need back, when
  it was run against this emulator: an address, a header, a member of an answer.
  It is recorded in `specs/vercel/clients.json` and `specs/vercel/api-extra.json`
  with the client and version that showed it, and kept as the request exactly as the
  client sent it (`test/behavior/vercel/testdata/clients`). It establishes that the
  client works here. It does not establish that api.vercel.com answers the same.
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

The CLI takes a token of letters, digits and underscores only (`--token` and
`VERCEL_TOKEN` alike are refused with "Must not contain: \"-\"" for anything else),
so `mirror_dev_token`, not `mirror-dev-token`.

```js
import { Vercel } from "@vercel/sdk";
const vercel = new Vercel({ bearerToken: "anything", serverURL: process.env.MIRROR_URL });
```

`scripts/vercel-acceptance.sh` is the whole deploy path in curl, and the thing to
read to see what a client sends: a project created at `v9` and at `v11`, a
variable, `POST /v2/files` with the SHA-1 in `x-vercel-digest`, `POST /v13/deployments`
naming that file, a poll to READY, an alias (refused until the account holds the
domain, then allowed), the deployment read back by id and by URL, and the project
deleted. `TestTheAcceptanceScriptDeploysAndItsAnswersAreTheDocuments`
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
to upload. A deployment that gives only a `name` nobody has made a project of creates
the project, where Vercel answers 404, and this is the one place the emulator is kinder;
one that names a `project` that does not exist is the 404.

Aliases need a `READY` deployment. Assigning a hostname that already names another
deployment moves it. A production deployment is aliased to `<project>.vercel.app`
once it is ready.

A hostname is the account's to alias only if it holds the domain: a `*.vercel.app` name
needs none, and any other host must be a domain of the account, sit under one (the apex,
which is the last two labels, three under the ccTLD second levels `apexOf` knows), or be a
hostname one of its projects holds. Otherwise the answer is the document's `404` ("The
domain used for the alias was not found"), under the code `domain_not_found`, which the
document does not name: the CLI itself lists it beside `invalid_domain` for a domain the
account cannot use, which makes it a reasoned choice and not an observed one. The check comes after the
hostname is known to be valid and before the deployment's state is looked at. Each team
holds its own domains, so one team cannot alias under another's.

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

## Pulling variables

`vercel env pull`, `vercel pull` and `vercel link` (which refreshes `VERCEL_OIDC_TOKEN` the same
way) read `GET /v3/env/pull/{project}[/{target}[/{gitBranch}]]`, a route the pinned document does
not list. It is authored from what the CLI does with the answer (vercel 63.1.2, read from its
JavaScript): it takes `env`, an object of `KEY: value`, and writes every key it is given. A
sensitive variable is the key with `""` (its value is not stored, so it cannot come back); the CLI
takes an empty value for one it could not read, asks `GET /v10/projects/{id}/env` which are
sensitive, and writes `[SENSITIVE]` in their place. The target is `development` (also what a pull
with no target gets), `preview`, `production`, or the slug or id of one of the project's custom
environments; anything else is a 400 rather than an empty file. A git branch adds the variables set
for it over the ones set for none, and without one they are left out. Nothing here was compared with
api.vercel.com: which variables a real target includes (system variables, for one) is not known.

## Domains the CLI adds to a project

`vercel domains add <domain>` adds a domain to the account (`POST /v7/domains`, the document's).
`vercel domains add <domain> <project>` does not use the document's `AddProjectDomain`: vercel
63.1.2 posts the older `POST /projects/{project}/alias` with `{"target": "PRODUCTION", "domain": host}`
and reads the entry for its host out of the array it gets back, so that is what the authored
`AddProjectAlias` answers (the entry's members beyond `domain` are not observed). The host lands in
the same store the document's routes use, so `GET /v9/projects/{id}/domains/{host}` finds it and
`AddProjectDomain` refuses it. When another project holds the host the answer is `409` with the code
`ALIAS_DOMAIN_EXIST` and `project: {id}` in the envelope, which is what the CLI reads to tell "already
on this project" from "on another", and what `--force` answers by taking the host off the holder
(`DELETE /projects/{id}/alias?domain=host`, `RemoveProjectAlias`) and posting again. The holder is named
by id only: a name kept beside it would go stale on a rename. `vercel domains inspect` and `rm` list the
projects under a domain with the document's `GET /v1/domains/{domain}/project-domains` (a 404 for a
domain the account does not hold), and `rm` deletes at `DELETE /v3/domains/{domain}`, an older address
of `DeleteDomain`. A domain still on a project is deleted all the same: the CLI knows a
`domain_removal_conflict`, and nothing here raises it.

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
No route is exempt in either mode: `CreateTeam`, `ListTeamMembers`, `UpdateAuthUser` and the
API-key routes, which the vendor's own emulator guards with a 401 of its own, are served to a
request with no credential like every other (`vercel_covenant_test.go` calls each with no
header, an arbitrary bearer, one that starts with `invalid` and basic credentials, in both
modes). The one 401 left is the OAuth `userinfo` route's, which is the protocol's and not a
credential check on the API.

## Persistence and determinism

`--seed` fixes the ids. The clock fixes the instants when it is controllable, and
otherwise the instants are the wall clock's. A transcript of the deploy path is
byte-identical across two runs of one seed on the controllable clock
(`TestVercelIsDeterministic`), and a restart from `--persist` keeps the project, its
variables, the deployments and the Edge Configs while later requests draw new ids
(`TestVercelStateSurvivesARestart`).

## Real clients

Vercel's CLI and its SDK have been run against the emulator, and what they sent is
kept. The versions, because the SDK validates every answer against a schema generated
from one revision of Vercel's document and the CLI's addresses move between releases:

| Client | Version | Run on |
|---|---|---|
| `vercel` (the CLI) | 63.1.2 | Linux x64, Node 22.22.0, from its JavaScript |
| `@vercel/sdk` | 1.28.43 | the same |

Both are client-observed (see the top of this page). Install them into a directory with
your package manager and name it (the CLI wants its dependencies there too, and the
journeys call `node_modules/.bin/vercel` and `node_modules/@vercel/sdk`):

```sh
MIRROR_VERCEL_CLIENT_DIR=~/vercel-clients go test ./test/behavior/vercel -run TestVercelsOwnClientsDeploy -v
```

Without the variable the test skips and says so. With it, both journeys
(`test/vercel-clients/`) pass: the SDK one in 4 to 7 s, the CLI one in 40 to 50 s (it walks about
forty commands).
The CLI's native binary (its default on macOS, opt-in on Linux) was not run. A newer
SDK is generated from a newer document than the one pinned here and may ask for more.

What each journey walks, and how it ends now:

| Step | Result |
|---|---|
| CLI `whoami`, `project add`, `link` | pass; `link` asks for the development variables to refresh `VERCEL_OIDC_TOKEN`, and none is issued |
| CLI `env add`, `env ls`, `env rm` | pass |
| CLI `deploy --prod`, `deploy` (preview) | pass; files go inline or by the upload protocol (`missing_files`, then `POST /v2/files`, the digest in `x-now-digest`) |
| CLI `inspect`, `ls`, `project ls`, `alias set\|ls\|rm`, `remove` | pass |
| CLI `redeploy`, `promote` | pass when run by hand; not in the journey script |
| SDK projects, variables, deployments, aliases (refused for a domain the account lacks, then allowed once it holds it with `createOrTransferDomain`), domains, Edge Config, and the listings that carry a pagination object | pass |
| CLI `env pull` (all three targets, a git branch), `pull` | pass; `GET /v3/env/pull/{project}[/{target}[/{branch}]]` is authored, the document does not list it (below) |
| CLI `build` | **not run**: it loads the framework builders, which the toolchain does not carry |
| CLI `domains add`, `domains add <domain> <project>` (also against a host another project holds, and `--force`), `domains ls`, `domains inspect`, `domains rm` | pass; the project form posts the older `POST /projects/{project}/alias`, authored (below) |
| CLI `alias set` to a `*.vercel.app` name, or to a domain the account holds | pass |
| CLI `alias set` to a hostname the account does not hold, or to a subdomain of one it does | **fail**: the CLI looks the name up as a domain (`GET /v5/domains/{host}`, a 404 for a subdomain) and then asks the registrar (`GET /v1/registrar/domains/{host}/availability`) whether it is for sale, and the registrar is not served. The API's own refusal is held by the Go tests, the acceptance script and the SDK journey |

What running them changed, one entry per finding (the commit that made each change
carries the request and the failure it fixed):

- The CLI creates a project at `POST /v1/projects`, not `/v11`.
- Several of the CLI's addresses carry **no version**, or the old `/now/` segment:
  `GET /projects/{id}/custom-environments`, `DELETE /projects/{id}`, `GET /teams/{id}`,
  `POST /now/deployments/{url}/aliases`, `GET /now/aliases/{alias}`,
  `DELETE /now/aliases/{id}`, `GET /v3/now/deployments/{id}/events`, `GET /v3/now/aliases`;
  and `project ls` reads `GET /v9/projects`. All are in `specs/vercel/clients.json`
  with the command that sends each. `vercel remove` treats a 404 on its DELETE as "already
  removed" and prints success, so an unserved address left the project in place.
- `vercel alias set` names the deployment by its **URL** in the path, not its id.
- The SDK requires a **`duration`** object on every deployment it reads; the pinned
  document does not list it (the SDK was published on 2026-10-09, the document pinned
  on 2026-09-11). Added with
  `x-mirror-members`, which lets the authored supplement add a member to a structure the
  pinned document declared, with its source on record.
- A personal account is addressed by the **user's own id**: `vercel link` writes it as the
  project's org and `vercel ls` sends it back as `?teamId=`.
- A **preview** deployment is `aliasAssigned` once it is READY. The CLI waits for it
  after READY and polled forever while it stayed false.
- `vercel inspect` reads `GET /v11/deployments/{id}/builds`, an operation the document
  does not list; it is authored (`GetDeploymentBuilds`) and answers an empty list, since
  nothing builds.
- `vercel env pull`, `vercel pull` and `vercel link`'s token refresh read
  `GET /v3/env/pull/{project}[/{target}[/{branch}]]`, which the document does not list
  (authored: `GetEnvPull`, `GetEnvPullTarget`, `GetEnvPullBranch`; see
  [Pulling variables](#pulling-variables)).
- `vercel domains add <domain> <project>` posts the older `POST /projects/{project}/alias`
  and keys on `ALIAS_DOMAIN_EXIST`; `domains inspect` and `rm` read the document's
  `GET /v1/domains/{domain}/project-domains`, and `rm` deletes at `DELETE /v3/domains/{domain}`
  (see [Domains the CLI adds to a project](#domains-the-cli-adds-to-a-project)).
- Not a client's finding but the document's: an alias to a hostname the account does not hold
  is the 404 "The domain used for the alias was not found", and the emulator accepted it. It
  refuses now (see [What a deployment does](#what-a-deployment-does)).

The recorded journeys are replayed without node or the clients by
`TestRealClientFixturesReplay`: every request exactly as it was sent, against a fresh
emulator on the client's own `Host` (so an address no model claims falls through to S3
and shows as `NoSuchBucket` instead of passing on a header the client does not send),
with the members of each answer the client went on to read. To record another journey,
point the client at a proxy in front of a running emulator that writes down each request
and answer, and add the steps to a file in `test/behavior/vercel/testdata/clients/<client>/`.

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
| `AddProjectAlias` | POST `/projects/{idOrName}/alias` | - | authored (held to the supplement's schema) |
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
| `DeleteAlias` | DELETE `/v2/aliases/{aliasId}` | `/now/aliases/{aliasId}`, `/v4/aliases/{aliasId}` | spec-validated |
| `DeleteAllArtifacts` | DELETE `/v8/artifacts` | - | spec-validated |
| `DeleteApiKey` | DELETE `/v1/api-keys/{keyId}` | - | authored (held to the supplement's schema) |
| `DeleteDeployment` | DELETE `/v13/deployments/{id}` | - | spec-validated |
| `DeleteDomain` | DELETE `/v6/domains/{domain}` | `/v3/domains/{domain}`, `/v4/domains/{domain}`, `/v5/domains/{domain}` | spec-validated |
| `DeleteEdgeConfig` | DELETE `/v1/global-config/{edgeConfigId}` | `/v1/edge-config/{edgeConfigId}` | spec-validated |
| `DeleteEdgeConfigSchema` | DELETE `/v1/global-config/{edgeConfigId}/schema` | `/v1/edge-config/{edgeConfigId}/schema` | spec-validated |
| `DeleteEdgeConfigTokens` | DELETE `/v1/global-config/{edgeConfigId}/tokens` | `/v1/edge-config/{edgeConfigId}/tokens` | spec-validated |
| `DeleteProject` | DELETE `/v9/projects/{idOrName}` | `/projects/{idOrName}` | spec-validated |
| `DeleteProjectCheck` | DELETE `/v2/projects/{projectIdOrName}/checks/{checkId}` | - | spec-validated |
| `DeleteRollingReleaseConfig` | DELETE `/v1/projects/{idOrName}/rolling-release/config` | - | spec-validated |
| `DownloadArtifact` | GET `/v8/artifacts/{hash}` | - | spec-validated; the body is bytes, so only the status is held |
| `EditProjectEnv` | PATCH `/v9/projects/{idOrName}/env/{id}` | `/v10/projects/{idOrName}/env/{id}`, `/v8/projects/{idOrName}/env/{id}` | spec-validated |
| `FilterProjectEnvs` | GET `/v10/projects/{idOrName}/env` | `/v8/projects/{idOrName}/env`, `/v9/projects/{idOrName}/env` | spec-validated |
| `GetAlias` | GET `/v4/aliases/{idOrAlias}` | `/now/aliases/{idOrAlias}`, `/v2/aliases/{idOrAlias}`, `/v3/aliases/{idOrAlias}` | spec-validated |
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
| `GetDomainProjectDomains` | GET `/v1/domains/{domain}/project-domains` | - | spec-validated |
| `GetDomainVerificationRecord` | GET `/v9/domains/{domain}/verification` | - | spec-validated |
| `GetDomains` | GET `/v5/domains` | `/v4/domains`, `/v6/domains` | spec-validated |
| `GetEdgeConfig` | GET `/v1/global-config/{edgeConfigId}` | `/v1/edge-config/{edgeConfigId}` | spec-validated |
| `GetEdgeConfigItem` | GET `/v1/global-config/{edgeConfigId}/item/{edgeConfigItemKey}` | `/v1/edge-config/{edgeConfigId}/item/{edgeConfigItemKey}` | spec-validated |
| `GetEdgeConfigItems` | GET `/v1/global-config/{edgeConfigId}/items` | `/v1/edge-config/{edgeConfigId}/items` | spec-validated |
| `GetEdgeConfigSchema` | GET `/v1/global-config/{edgeConfigId}/schema` | `/v1/edge-config/{edgeConfigId}/schema` | spec-validated |
| `GetEdgeConfigToken` | GET `/v1/global-config/{edgeConfigId}/token/{token}` | `/v1/edge-config/{edgeConfigId}/token/{token}` | spec-validated |
| `GetEdgeConfigTokens` | GET `/v1/global-config/{edgeConfigId}/tokens` | `/v1/edge-config/{edgeConfigId}/tokens` | authored (held to the supplement's schema) |
| `GetEdgeConfigs` | GET `/v1/global-config` | `/v1/edge-config` | authored (held to the supplement's schema) |
| `GetEnvPull` | GET `/v3/env/pull/{idOrName}` | - | authored (held to the supplement's schema) |
| `GetEnvPullBranch` | GET `/v3/env/pull/{idOrName}/{target}/{gitBranch}` | - | authored (held to the supplement's schema) |
| `GetEnvPullTarget` | GET `/v3/env/pull/{idOrName}/{target}` | - | authored (held to the supplement's schema) |
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
| `RemoveProjectAlias` | DELETE `/projects/{idOrName}/alias` | - | authored (held to the supplement's schema) |
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
Each is a thing that is not served or not known, said as such; nothing here was
compared with api.vercel.com.

- **No build, no serving.** A deployment's files are recorded and its URL answers
  nothing. `GetDeploymentFileContents` is an empty object; a file's bytes are not
  kept (the remote cache's are). `vercel build` was not run: it loads the framework
  builders, which the toolchain does not carry, and a deployment is made from source
  (`vercel deploy`), not from prebuilt output.
- **Build logs do not stream.** `GET /v3/deployments/{id}/events` answers the events
  the emulator records (created, building, ready, canceled) as a finite list;
  `direction`, `limit`, `since` and `until` are read, `follow=1` and the other query
  members are accepted and read by nothing, and there is no build output to follow.
- **About 310 of the document's 417 operations are not served** (103 are): the marketplace
  and integrations, log drains and webhooks, sandboxes, storage, firewall, feature
  flags, certificates, DNS records, domain purchase and transfer (the registrar),
  access groups, shared variables, and the rest. They answer 501 when sent with
  `Host: api.vercel.com`; a client pointed at `http://127.0.0.1:PORT` sends its own
  host, and an address no model claims then falls through to S3 and answers its
  `NoSuchBucket`.
- **Pulling variables** (above) is the project's own variables for one target. A pull of a
  deployment id (`env pull --id dpl_...`, which reads `buildEnv`) is not served, no
  `VERCEL_OIDC_TOKEN` is issued, and which variables beyond the project's own a real
  target includes (system variables, for one) is not known. A sensitive variable comes
  back as its key with `""`; whether a real one is that or absent is not known either.
- **The registrar is not served**, so `vercel alias set` to a hostname the account does
  not hold, or to a subdomain of one it does, ends there: the CLI looks the name up as a
  domain (`GET /v5/domains/{host}`, a 404), then asks whether it is for sale. What the
  real CLI does for a subdomain of a domain it holds is not known. `vercel domains
  inspect` asks the registrar for a renewal price, gets a 404 and prints `-`. The alias
  route itself refuses an unheld hostname, and `vercel alias set` to a `*.vercel.app` name
  or to a domain the account holds reaches it.
- **Domains.** A domain is never resolved: verifying a project domain takes the TXT
  challenge as met, there is no DNS to ask, and a held domain counts for an alias whether
  or not it is verified. A domain still on a project is deleted all the same; the CLI knows a
  `domain_removal_conflict` that nothing raises. `domains add --force` falls back on
  `GET /project-domains/{domain}` when the error does not name the holder; the error
  always does, so that address is not served. An alias target's members beyond `domain`,
  and what `DELETE /projects/{id}/alias` answers, are authored.
- **Edge Config:** the control plane only. Its data plane
  (`edge-config.vercel.com`, what an application reads through with a token) is
  another host and another product. Backups are not served. A config's schema is
  stored and returned, never enforced against its items. The digest is a hash of the
  config's id and a revision, not of its items.
- **Rolling releases** are a state machine a client steps through; no traffic is
  split. Billing is "unlimited".
- **Teams** are created, read, renamed and given members by email; there is no
  invitation to accept, no roles beyond the stored string, no billing.
- **Pagination.** `pagination.next` is a number, as the document types it, and carries more
  than the timestamp: the creation instant of the last item on the page with that item's
  place among those made in the same millisecond as the fraction (a 1/1024 step). Sent back
  as `until` (`from` for projects) it continues without a skip or a repeat, which a bare
  timestamp cannot do for items that share one, and every item does on a controllable
  clock that is not moving. A bare integer timestamp still means what it did (the items
  strictly before it for `until`, after it for `since`), and anything that is not a number
  is a 400. The ceiling is 1023: more items than that in one millisecond are ambiguous at
  a page boundary again. Projects page by creation, since that is the order of the listing
  (an update moves nothing in it); the document words `from` as "updated after". The
  paged listings are teams, projects, deployments (`/v6` and `/v7`), aliases, the
  account's domains, a project's domains and the project domains of an apex; the others
  (project variables, team members, Edge Config) answer one page with `next: null`.
- **The OAuth flow** is the oracle's local login (a page, a callback, a token
  exchange, userinfo), not Vercel's.
- **Rate limiting** is the armed 429s and nothing else; there is no accounting.
