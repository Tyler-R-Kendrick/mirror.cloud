package scheduler

import (
	"testing"
	"time"
)

func TestScheduleExpressions(t *testing.T) {
	utc := func(raw string) time.Time {
		t.Helper()
		v, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	tests := []struct {
		expression, timezone, after, want string
	}{
		{"at(2024-02-29T12:30:00)", "UTC", "2024-02-29T12:00:00Z", "2024-02-29T12:30:00Z"},
		{"cron(*/15 9-10 ? JAN MON-FRI 2024)", "UTC", "2024-01-01T09:01:00Z", "2024-01-01T09:15:00Z"},
		{"cron(0 9 L * ? 2024)", "UTC", "2024-02-01T00:00:00Z", "2024-02-29T09:00:00Z"},
		{"cron(0 9 1W * ? 2024)", "UTC", "2024-05-31T09:00:00Z", "2024-06-03T09:00:00Z"},
		{"cron(0 9 ? * MON#2 2024)", "UTC", "2024-01-01T00:00:00Z", "2024-01-08T09:00:00Z"},
		{"cron(0 9 ? * FRIL 2024)", "UTC", "2024-02-01T00:00:00Z", "2024-02-23T09:00:00Z"},
		{"cron(0 9 ? * MON-FRI 2024)", "America/Chicago", "2024-03-10T14:00:00Z", "2024-03-11T14:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			expr, err := parseScheduleExpression(tt.expression, tt.timezone)
			if err != nil {
				t.Fatal(err)
			}
			got := expr.first(utc(tt.after), time.Time{})
			if !got.Equal(utc(tt.want)) {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestScheduleExpressionValidation(t *testing.T) {
	bad := []string{
		"rate(0 minutes)", "rate(1 minutes)", "rate(2 minute)",
		"cron(? 0 * * ? 2024)", "cron(0 0 * * * 2024)", "cron(0 0 ? 13 MON 2024)", "cron(0 0 ? * MON#6 2024)",
	}
	for _, raw := range bad {
		if _, err := parseScheduleExpression(raw, "UTC"); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
	if _, err := parseScheduleExpression("rate(1 minute)", "Not/A_Zone"); err == nil {
		t.Fatal("invalid timezone accepted")
	}
}

func TestAtExpressionIgnoresStartDate(t *testing.T) {
	expr, err := parseScheduleExpression("at(2024-01-02T00:00:00)", "UTC")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := expr.first(now, now.Add(48*time.Hour)); !got.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("got %s", got)
	}
}
