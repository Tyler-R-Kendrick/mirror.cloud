package journal_test

import (
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/journal"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestRecordQuery(t *testing.T) {
	j := journal.New()
	t0 := time.Unix(0, 0).UTC()
	t1 := t0.Add(time.Hour)
	t2 := t1.Add(time.Hour)
	j.Record(spi.Entry{At: t0, ServiceID: "aws.s3", Operation: "PutObject", Tier: model.TierEmulate})
	j.Record(spi.Entry{At: t1, ServiceID: "aws.s3", Operation: "GetObject"})
	j.Record(spi.Entry{At: t2, ServiceID: "aws.sqs", Operation: "SendMessage"})

	all := j.Query(spi.Filter{})
	if len(all) != 3 || all[0].Operation != "SendMessage" {
		t.Fatalf("newest first: %+v", all)
	}

	s3 := j.Query(spi.Filter{ServiceID: "aws.s3"})
	if len(s3) != 2 {
		t.Fatalf("service filter: %+v", s3)
	}
	put := j.Query(spi.Filter{Operation: "PutObject"})
	if len(put) != 1 || put[0].Operation != "PutObject" {
		t.Fatalf("op filter: %+v", put)
	}
	since := j.Query(spi.Filter{Since: t1})
	if len(since) != 2 {
		t.Fatalf("since: %+v", since)
	}
	limited := j.Query(spi.Filter{Limit: 1})
	if len(limited) != 1 || limited[0].ServiceID != "aws.sqs" {
		t.Fatalf("limit: %+v", limited)
	}
}
