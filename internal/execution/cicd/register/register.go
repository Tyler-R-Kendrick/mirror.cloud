// Package register blank-imports CI/CD executors so AfterInvoke hooks register.
package register

import (
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/amplify"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codedeploy"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codepipeline"
)
