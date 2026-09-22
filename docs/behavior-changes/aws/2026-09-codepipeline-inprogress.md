# CodePipeline — false-success repair (2026-09)

StartPipelineExecution admits `InProgress` (not `Succeeded`). GetPipelineState
returns nonempty `stageStates` from the pipeline declaration. ListPipelineExecutions
filters by `pipelineName`. StopPipelineExecution only rewrites InProgress
executions (Stopped vs Abandoned). Equivalence trace recut accordingly.
