package engine

import (
	"context"
	"fmt"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// runBatch delegates one request to a sibling operation, once per entry.
//
// AWS batch operations all have the same shape -- a list of entries each
// carrying a caller-chosen Id, answered by a Successful list and a Failed list
// whose rows carry that Id plus the error -- and the hand-written packs
// implemented that shape once per batch operation. Each copy was then free to
// diverge from the singular operation it was supposed to mirror, and several
// did. Doing it here means SendMessageBatch cannot drift from SendMessage,
// because it *is* SendMessage.
//
// A failing entry does not fail the batch: that is the semantics AWS defines,
// and it is what makes the Failed list meaningful. Only an error that is not a
// fault -- a store failure, a bundle bug -- stops the whole request.
func (e *Engine) runBatch(ctx context.Context, req *spi.Request, op bir.Operation) (*spi.Response, error) {
	b := op.Batch
	if _, ok := e.ir.Operations[b.Of]; !ok {
		return nil, fmt.Errorf("engine: %s.batch: %q is not an operation of this service",
			req.Operation, b.Of)
	}

	entries, _ := req.Input[b.Entries].([]any)
	successful := []any{}
	failed := []any{}

	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if entry == nil {
			continue
		}
		id := entry[b.ID]

		sub := &spi.Request{
			ServiceID: req.ServiceID,
			Operation: b.Of,
			Identity:  req.Identity,
			HTTP:      req.HTTP,
			Input:     map[string]any{},
		}
		// What the batch addresses as a whole -- the queue every entry goes to
		// -- travels with each delegated request.
		for _, member := range b.Carry {
			if v, ok := req.Input[member]; ok {
				sub.Input[member] = v
			}
		}
		for k, v := range entry {
			if k == b.ID {
				continue
			}
			sub.Input[k] = v
		}

		resp, err := e.Invoke(ctx, sub)
		if err != nil {
			fault, ok := err.(*spi.Fault)
			if !ok {
				return nil, err
			}
			failed = append(failed, map[string]any{
				"Id":          id,
				"Code":        fault.Code,
				"Message":     fault.Message,
				"SenderFault": fault.Fault == "client",
			})
			continue
		}
		row := map[string]any{"Id": id}
		for _, member := range b.Result {
			if resp != nil {
				if v, ok := resp.Output[member]; ok {
					row[member] = v
				}
			}
		}
		successful = append(successful, row)
	}

	out := map[string]any{}
	if b.Successful != "" {
		out[b.Successful] = successful
	}
	if b.Failed != "" {
		out[b.Failed] = failed
	}
	return &spi.Response{Output: out}, nil
}
