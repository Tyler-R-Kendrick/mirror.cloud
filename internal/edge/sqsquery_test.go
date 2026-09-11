package edge

import (
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// TestAQueueURLEndpointServesQueueActions pins which actions a client may send
// to a queue's own URL, and takes the answer from the model.
//
// An operation whose input declares QueueUrl is about a queue, so the URL in
// the path supplies it. CreateQueue and ListQueues declare none, because they
// address the account -- a client that sends either to a queue URL has made a
// mistake the queue in the path cannot resolve.
func TestAQueueURLEndpointServesQueueActions(t *testing.T) {
	// The generated bundle, which is what the edge serves: catalog.Bundle()
	// records no shapes, and a rule that reads the model needs a model.
	svc := specboot.Bundle().ServiceByID("aws.sqs")
	if svc == nil {
		t.Fatal("aws.sqs is not in the bundle")
	}
	for _, action := range []string{
		"SendMessage", "ReceiveMessage", "DeleteMessage", "DeleteMessageBatch",
		"ChangeMessageVisibility", "GetQueueAttributes", "SetQueueAttributes",
		"DeleteQueue", "PurgeQueue", "ListQueueTags", "TagQueue", "UntagQueue",
		"AddPermission", "RemovePermission", "ListDeadLetterSourceQueues",
		// The documented exception: GetQueueUrl takes a name rather than a URL
		// because it is how a client obtains one, and AWS answers it at any SQS
		// endpoint including a queue's own.
		"GetQueueUrl",
	} {
		if err := sqsQueueEndpointAction(svc, action); err != nil {
			t.Errorf("%s rejected at a queue URL: %v", action, err)
		}
	}
	for _, action := range []string{"CreateQueue", "ListQueues", "FooBar", ""} {
		err := sqsQueueEndpointAction(svc, action)
		fault, ok := err.(*spi.Fault)
		if !ok {
			t.Errorf("%s accepted at a queue URL", action)
			continue
		}
		if fault.Code != "InvalidAction" || fault.HTTPStatus != 400 {
			t.Errorf("%s rejected as %s/%d", action, fault.Code, fault.HTTPStatus)
		}
		if want := "The action " + action + " is not valid for this endpoint."; fault.Message != want {
			t.Errorf("%s message %q, want %q", action, fault.Message, want)
		}
	}
}

// TestTheQueueEndpointRuleReadsTheModel fails if the QueueUrl member stops
// being what decides, rather than silently falling back to a list someone has
// to maintain. A hard-coded set is how the demux accumulated a hundred and
// thirty branches.
func TestTheQueueEndpointRuleReadsTheModel(t *testing.T) {
	svc := specboot.Bundle().ServiceByID("aws.sqs")
	if svc == nil {
		t.Fatal("aws.sqs is not in the bundle")
	}
	op := svc.OperationByName("SendMessage")
	if op == nil {
		t.Fatal("aws.sqs has no SendMessage")
	}
	shape, ok := svc.Shapes[op.Input]
	if !ok {
		t.Fatalf("SendMessage input %q is not in the model", op.Input)
	}
	if _, declared := shape.Members["QueueUrl"]; !declared {
		t.Fatal("SendMessage no longer declares QueueUrl; the rule needs rederiving")
	}
	stripped := *svc
	stripped.Shapes = map[string]model.Shape{}
	for id, s := range svc.Shapes {
		stripped.Shapes[id] = s
	}
	without := shape
	without.Members = map[string]model.Member{}
	for name, member := range shape.Members {
		if name != "QueueUrl" {
			without.Members[name] = member
		}
	}
	stripped.Shapes[op.Input] = without
	if err := sqsQueueEndpointAction(&stripped, "SendMessage"); err == nil {
		t.Error("SendMessage still accepted with QueueUrl removed from the model; " +
			"the rule is not reading the model")
	}

	// A model that records no shapes at all cannot say whether SendMessage is
	// about a queue, and silence is not a "no". catalog.Bundle() is exactly
	// this shape, so reading it as a rejection would take every queue action
	// down at every queue URL -- strictly worse than not checking.
	unanswerable := *svc
	unanswerable.Shapes = nil
	for _, action := range []string{"SendMessage", "ReceiveMessage", "CreateQueue", "ListQueues"} {
		if err := sqsQueueEndpointAction(&unanswerable, action); err != nil {
			t.Errorf("%s rejected by a model that records no shapes: %v", action, err)
		}
	}
	// An action the service does not declare at all is still rejected: that
	// answer does not need shapes.
	if err := sqsQueueEndpointAction(&unanswerable, "FooBar"); err == nil {
		t.Error("FooBar accepted by a model that records no shapes")
	}
}

// TestUnknownOperationIsTheBareAWSElement keeps the body AWS actually returns.
// A client matching on `<UnknownOperationException` finds nothing in a
// `<Code>UnknownOperationException</Code>` envelope.
func TestUnknownOperationIsTheBareAWSElement(t *testing.T) {
	if !strings.HasPrefix(sqsUnknownOperation, "<UnknownOperationException") {
		t.Errorf("unknown-operation body %q is not the bare element", sqsUnknownOperation)
	}
	if strings.Contains(sqsUnknownOperation, "ErrorResponse") {
		t.Errorf("unknown-operation body %q is wrapped", sqsUnknownOperation)
	}
}
