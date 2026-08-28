package events

import "testing"

func TestMatchEventPatternOperators(t *testing.T) {
	event := map[string]any{
		"source": "app.orders",
		"tags":   []any{"new", "priority"},
		"detail": map[string]any{
			"price": 15.0, "ip": "10.2.3.4", "status": "created",
		},
	}
	tests := []struct {
		name, pattern string
		want          bool
	}{
		{"operators", `{"source":[{"prefix":"app."}],"tags":["priority"],"detail":{"price":[{"numeric":[">",10,"<=",20]}],"ip":[{"cidr":"10.0.0.0/8"}],"status":[{"anything-but":["failed","cancelled"]}],"missing":[{"exists":false}]}}`, true},
		{"nested mismatch", `{"detail":{"price":[{"numeric":[">",20]}]}}`, false},
		{"or", `{"$or":[{"source":["other"]},{"source":["app.orders"]}]}`, true},
		{"suffix", `{"source":[{"suffix":".orders"}]}`, true},
		{"ignore case", `{"detail":{"status":[{"equals-ignore-case":"CREATED"}]}}`, true},
		{"wildcard", `{"source":[{"wildcard":"app.*ers"}]}`, true},
		{"wildcard anchored", `{"source":[{"wildcard":"orders*"}]}`, false},
		{"invalid", `{`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchEventPattern(tt.pattern, event); got != tt.want {
				t.Fatalf("matchEventPattern() = %v, want %v", got, tt.want)
			}
		})
	}
}
