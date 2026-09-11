package edge

import (
	"net/http"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// SQS is reachable two ways, and the two disagree about what a request that
// does not name a valid operation is.
//
// The JSON endpoint takes the operation from X-Amz-Target and is routed like
// every other awsJson service. The query endpoint takes it from an `Action`
// parameter, and it is also *addressed per queue*: a client holds a queue URL
// like `https://sqs.us-east-1.amazonaws.com/000000000000/orders` and sends
// every subsequent call to that path. AWS's frontend treats such a path as
// naming the queue, which means two request shapes have answers nothing in the
// generic query codec could produce.
//
// Neither is a Mirror invention; both are what a client sees against AWS.

// sqsUnknownOperation is the body AWS returns for a query-endpoint request
// that names no action at all: a bare element, at 404, with no ErrorResponse
// envelope around it. A client that pointed itself at an SQS endpoint and sent
// nothing recognisable gets this rather than a modelled fault, because the
// frontend rejects it before any operation is chosen.
const sqsUnknownOperation = `<UnknownOperationException/>`

// sqsQueueEndpointException is the fault AWS returns for an action that is
// real but not addressable at a queue URL.
//
// Which actions those are comes from the model rather than a list: an
// operation whose input declares QueueUrl is *about* a queue, so a queue URL
// supplies it, and an operation that declares no QueueUrl addresses the
// account. CreateQueue and ListQueues are the second kind, and a client that
// sends either to a queue URL has made a real mistake -- the queue in the path
// cannot be the queue it means.
//
// GetQueueUrl is the documented exception, and it is why this is not a plain
// `declares QueueUrl` check. It takes a name rather than a URL because it is
// how a client *obtains* a URL, and AWS answers it at any SQS endpoint
// including a queue's own. TestBootedServerSQSSection48 pins both halves: asked
// at `/000000000000/queryq` for `queryq2`, it answers with queryq2's URL and
// not with the one in the path.
// The check fails open on a model that cannot answer. A service whose shapes
// did not load records no input for any operation, and reading that silence as
// "declares no QueueUrl" would reject SendMessage -- every queue action, at
// every queue URL. A rule derived from the model has to be able to tell an
// absent model from a negative answer, so only a shape that is present and
// lacks the member rejects anything.
func sqsQueueEndpointAction(svc *model.Service, action string) error {
	if action == "GetQueueUrl" {
		return nil
	}
	if op := svc.OperationByName(action); op != nil {
		shape, ok := svc.Shapes[op.Input]
		if !ok {
			return nil
		}
		if _, declared := shape.Members["QueueUrl"]; declared {
			return nil
		}
	}
	return &spi.Fault{
		Code:       "InvalidAction",
		Message:    "The action " + action + " is not valid for this endpoint.",
		HTTPStatus: http.StatusBadRequest,
		Fault:      "client",
	}
}
