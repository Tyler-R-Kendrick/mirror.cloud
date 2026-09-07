package engine_test

import (
	"fmt"
	"testing"
)

// An AWS batch response is correlated to its request by position: the rows
// carry an index into the list that was sent, not a caller-supplied id. CEL's
// `map` binds the element and never its position, so a bundle could answer the
// right number of rows and not say which input each one was about.
//
// `indices` is the whole of the fix, and these hold what it has to do: one
// index per element, in order, from zero, and nothing at all for an empty
// list.

func sentiments(t *testing.T, out map[string]any) []any {
	t.Helper()
	list, ok := out["ResultList"].([]any)
	if !ok {
		t.Fatalf("ResultList is %#v, not a list", out["ResultList"])
	}
	return list
}

func TestIndicesNumbersEveryRowOfABatch(t *testing.T) {
	p := served(t, "aws.comprehend")
	out := invoke(t, p, "BatchDetectSentiment", map[string]any{
		"LanguageCode": "en",
		"TextList":     []any{"first", "second", "third"},
	})
	rows := sentiments(t, out)
	if len(rows) != 3 {
		t.Fatalf("%d rows for 3 texts: %v", len(rows), rows)
	}
	for i, row := range rows {
		rec, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("row %d is %#v, not a record", i, row)
		}
		// The index has to be the position, not merely present: a row
		// numbered wrong points a caller at the wrong text.
		if got := fmt.Sprint(rec["Index"]); got != fmt.Sprint(i) {
			t.Errorf("row %d carries Index %s", i, got)
		}
		if rec["Sentiment"] != "POSITIVE" {
			t.Errorf("row %d carries Sentiment %v", i, rec["Sentiment"])
		}
	}
	if errs, _ := out["ErrorList"].([]any); len(errs) != 0 {
		t.Errorf("ErrorList is not empty: %v", errs)
	}
}

func TestIndicesOfAnEmptyListIsEmpty(t *testing.T) {
	p := served(t, "aws.comprehend")
	out := invoke(t, p, "BatchDetectSentiment", map[string]any{
		"LanguageCode": "en",
		"TextList":     []any{},
	})
	if rows := sentiments(t, out); len(rows) != 0 {
		t.Errorf("%d rows for no texts: %v", len(rows), rows)
	}
}
