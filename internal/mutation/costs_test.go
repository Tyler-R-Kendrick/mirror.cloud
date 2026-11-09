package mutation

// mutantCost is the CPU seconds (user + system of the go test it runs) one
// mutant of each package took: one mutant per package, measured twice on
// 2026-11-09 on a shared 4-CPU host (load average 30 to 47) with a warm build
// cache, and averaged. The compile and link of the mutated package's test
// binary is most of it, so the cost follows the package and what links it,
// not the mutant. A package not listed costs defaultMutantCost, the median.
//
// assign fills shards by these, and TestMutationPlan sizes a pull request's
// matrix by their sum. Every run's report (MUTATION_REPORT, the step summary)
// prints the measured cost beside the table's; update a number when the two
// drift apart.
var mutantCost = map[string]float64{
	"./cmd/mirror":                           10.1,
	"./cmd/mirrorgen":                        5.6,
	"./internal/bir":                         1.4,
	"./internal/bundled":                     6.8,
	"./internal/chaos":                       7.9,
	"./internal/config":                      0.6,
	"./internal/conformance":                 11.6,
	"./internal/edge":                        6.7,
	"./internal/engine":                      2.0,
	"./internal/equivalence":                 6.9,
	"./internal/identity":                    0.8,
	"./internal/mock":                        0.6,
	"./internal/model":                       0.4,
	"./internal/proto/aws/awsquery":          0.6,
	"./internal/proto/aws/httpuri":           0.8,
	"./internal/proto/aws/restjson":          1.1,
	"./internal/proto/aws/restxml":           1.5,
	"./internal/proto/gcp/gcprest":           0.6,
	"./internal/proto/graphql":               0.9,
	"./internal/receiver/aws/smithy":         0.3,
	"./internal/registry":                    8.8,
	"./internal/services/aws/apigateway":     1.5,
	"./internal/services/aws/cloudcontrol":   6.2,
	"./internal/services/aws/cloudformation": 6.2,
	"./internal/services/aws/dynamodb":       7.0,
	"./internal/services/aws/dynamodb/expr":  0.5,
	"./internal/services/aws/ecs":            6.7,
	"./internal/services/aws/eventhttp":      0.6,
	"./internal/services/aws/events":         22.0,
	"./internal/services/aws/firehose":       8.2,
	"./internal/services/aws/iam":            4.2,
	"./internal/services/aws/kafka":          7.2,
	"./internal/services/aws/kinesis":        6.0,
	"./internal/services/aws/kms":            5.9,
	"./internal/services/aws/lambda":         17.6,
	"./internal/services/aws/organizations":  6.7,
	"./internal/services/aws/pipes":          6.4,
	"./internal/services/aws/redshift":       9.7,
	"./internal/services/aws/s3":             14.8,
	"./internal/services/aws/s3tables":       5.9,
	"./internal/services/aws/scheduleexpr":   0.3,
	"./internal/services/aws/scheduler":      6.5,
	"./internal/services/aws/sns":            7.7,
	"./internal/services/aws/states":         8.9,
	"./internal/services/gcp/gcs":            6.7,
	"./internal/specboot":                    4.8,
	"./internal/spine":                       7.7,
	"./internal/store":                       10.7,
	"./test/behavior/aws":                    10.6,
	"./test/behavior/vercel":                 7.9,
}

const defaultMutantCost = 6.4
