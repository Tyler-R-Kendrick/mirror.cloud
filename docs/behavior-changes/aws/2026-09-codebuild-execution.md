# CodeBuild — false-success repair and local execute (2026-09)

## Evidence

- AWS CodeBuild StartBuild previously persisted `buildStatus: SUCCEEDED` with no
  source fetch, buildspec parse, or command execution (`behavior/aws/codebuild`
  quirks; equivalence trace `internal/equivalence/traces/aws.codebuild.json`).
- AWS docs: build status vs phase enums; StopBuild retains terminal status;
  DeleteProject retains build history.

## Behavioral corrections

| Operation | Before | After |
|---|---|---|
| StartBuild | Immediate SUCCEEDED | IN_PROGRESS admission; optional process executor when `MIRROR_CICD_EXECUTE=1` |
| StartBuild missing project | Succeeded against ghost project | ResourceNotFoundException |
| UpdateProject | Nulls omitted fields; creates missing | Merge patch; ResourceNotFoundException if absent |
| CreateProject duplicate | Silent replace | ResourceAlreadyExistsException |
| StopBuild missing | Empty success | ResourceNotFoundException |
| StopBuild terminal | Rewrote to STOPPED | Unchanged final status |
| BatchGet* | No NotFound members | `projectsNotFound` / `buildsNotFound` |

## Equivalence

Trace `aws.codebuild.json` recut with `recut` citations on corrected steps.
