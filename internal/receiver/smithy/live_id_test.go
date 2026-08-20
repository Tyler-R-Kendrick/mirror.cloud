package smithy

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestLiveIDs(t *testing.T) {
	paths := []string{
		"../../../specs/aws/models/cloudwatch/service/2010-08-01/cloudwatch-2010-08-01.json",
		"../../../specs/aws/models/ecr/service/2015-09-21/ecr-2015-09-21.json",
		"../../../specs/aws/models/elastic-load-balancing-v2/service/2015-12-01/elastic-load-balancing-v2-2015-12-01.json",
		"../../../specs/aws/models/application-auto-scaling/service/2016-02-06/application-auto-scaling-2016-02-06.json",
		"../../../specs/aws/models/resource-groups-tagging-api/service/2017-01-26/resource-groups-tagging-api-2017-01-26.json",
	}
	var r Receiver
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(p, err)
		}
		svcs, err := r.Ingest(context.Background(), model.SourceRef{Path: p}, data)
		if err != nil {
			t.Fatal(p, err)
		}
		for _, s := range svcs {
			fmt.Printf("ID=%s ep=%s ns=%s proto=%s ops=%d\n", s.ID, s.EndpointPrefix, s.Namespace, s.Protocol, len(s.Operations))
		}
	}
}
