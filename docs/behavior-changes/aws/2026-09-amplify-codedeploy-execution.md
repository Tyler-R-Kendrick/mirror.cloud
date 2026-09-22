# Amplify / CodeDeploy — false-success repair and local execute (2026-09)

## Evidence

- Amplify `StartJob` persisted `status: SUCCEED` with no buildSpec run.
- CodeDeploy `CreateDeployment` persisted `status: Succeeded` with no AppSpec
  hooks or instance contact.

## Behavioral corrections

| Operation | Before | After |
|---|---|---|
| Amplify StartJob | Immediate SUCCEED | RUNNING admission; optional process executor (`MIRROR_CICD_EXECUTE=1` / `amplify.Enable`) |
| CodeDeploy CreateDeployment | Immediate Succeeded | InProgress admission; optional local Linux target (`codedeploy.Enable`) |

## Fixture

`internal/execution/cicd/awsdelivery` — AWS-DELIVERY composite (CodeBuild →
CodeDeploy HTTP verify; Amplify preview/prod isolation; failed build does not
advance production).
