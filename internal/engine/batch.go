package engine

import (
	"context"
	"fmt"
	"strconv"
	"unicode"

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

	rawEntries, _ := req.Input[b.Entries].([]any)
	if b.EmptyError != "" && len(rawEntries) == 0 {
		return nil, e.fault(b.EmptyError, "")
	}
	if b.MaxEntries > 0 && len(rawEntries) > b.MaxEntries {
		msg := fmt.Sprintf("Maximum number of entries per request are %d. You have sent %d.",
			b.MaxEntries, len(rawEntries))
		errName := b.TooManyError
		if errName == "" {
			errName = "TooManyEntriesInBatchRequest"
		}
		f := e.fault(errName, msg)
		return nil, f
	}

	entries := make([]any, len(rawEntries))
	copy(entries, rawEntries)
	if b.BareField != "" {
		for i, entry := range entries {
			if s, ok := entry.(string); ok && s != "" {
				entries[i] = map[string]any{b.ID: strconv.Itoa(i), b.BareField: s}
			}
		}
	}
	if b.MaxBytes > 0 {
		total := 0
		for _, raw := range entries {
			entry, _ := raw.(map[string]any)
			if entry == nil {
				continue
			}
			total += messageByteSize(fmt.Sprint(entry["MessageBody"]), entry["MessageAttributes"])
		}
		if total > b.MaxBytes {
			errName := b.TooLongError
			if errName == "" {
				errName = "BatchRequestTooLong"
			}
			msg := fmt.Sprintf("Batch requests cannot be longer than %d bytes. You have sent %d bytes.", b.MaxBytes, total)
			return nil, e.fault(errName, msg)
		}
	}
	if b.InvalidIDError != "" {
		for _, raw := range entries {
			entry, _ := raw.(map[string]any)
			if entry == nil {
				continue
			}
			if !validBatchEntryID(fmt.Sprint(entry[b.ID])) {
				return nil, e.fault(b.InvalidIDError,
					"A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.")
			}
		}
	}

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
	if b.Failed != "" && len(failed) > 0 {
		out[b.Failed] = failed
	}
	return &spi.Response{Output: out}, nil
}

func validBatchEntryID(id string) bool {
	if id == "" || len(id) > 80 {
		return false
	}
	for _, r := range id {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func messageByteSize(body string, attrs any) int {
	size := len([]byte(body))
	m, _ := attrs.(map[string]any)
	for name, raw := range m {
		attribute, _ := raw.(map[string]any)
		if attribute == nil {
			continue
		}
		size += len(name) + len(anyString(attribute["DataType"])) +
			len(anyString(attribute["StringValue"])) + len(anyString(attribute["BinaryValue"]))
	}
	return size
}

func anyString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
