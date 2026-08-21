package smithy

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestIngestService(t *testing.T) {
	src := `{
	  "smithy": "2.0",
	  "shapes": {
	    "com.example#Demo": {
	      "type": "service",
	      "operations": [{"target": "com.example#Ping"}],
	      "traits": {
	        "aws.protocols#awsJson1_0": {},
	        "aws.api#service": {"endpointPrefix": "demo", "sdkId": "Demo"}
	      }
	    },
	    "com.example#Ping": {
	      "type": "operation",
	      "input": {"target": "com.example#PingInput"},
	      "output": {"target": "com.example#PingOutput"},
	      "traits": {"smithy.api#readonly": {}, "smithy.api#http": {"method": "POST", "uri": "/", "code": 200}}
	    },
	    "com.example#PingInput": {
	      "type": "structure",
	      "members": {"Name": {"target": "smithy.api#String", "traits": {"smithy.api#required": {}}}}
	    },
	    "com.example#PingOutput": {"type": "structure", "members": {}}
	  }
	}`
	svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "demo.json"}, []byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if len(svcs) != 1 || svcs[0].ID != "aws.demo" {
		t.Fatalf("%+v", svcs)
	}
	if len(svcs[0].Operations) != 1 || svcs[0].Operations[0].Name != "Ping" {
		t.Fatalf("ops %+v", svcs[0].Operations)
	}
	if !svcs[0].Operations[0].Readonly {
		t.Fatal("readonly")
	}
}

func TestDetect(t *testing.T) {
	if !(Receiver{}).Detect("x.json", []byte(`{"smithy":"2.0"}`)) {
		t.Fatal("detect")
	}
}
