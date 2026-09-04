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

// TestIngestRecordsTheSigningName covers the name a client puts in its SigV4
// credential scope. For seventy-seven upstream models it is not the endpoint
// prefix -- Lex Model Building signs as `lex` and is reached at `models.lex`
// -- so a server that knows only the prefix cannot recognise the request.
//
// It is recorded as an alias rather than replacing the prefix, because both
// are real: the host carries one and the Authorization header the other.
func TestIngestRecordsTheSigningName(t *testing.T) {
	model_ := func(prefix, signing string) string {
		sig := ""
		if signing != "" {
			sig = `"aws.auth#sigv4": {"name": "` + signing + `"},`
		}
		return `{
		  "smithy": "2.0",
		  "shapes": {
		    "com.example#Demo": {
		      "type": "service",
		      "operations": [],
		      "traits": {
		        "aws.protocols#restJson1": {},
		        ` + sig + `
		        "aws.api#service": {"endpointPrefix": "` + prefix + `", "sdkId": "Demo"}
		      }
		    }
		  }
		}`
	}
	ingest := func(t *testing.T, doc string) model.Service {
		t.Helper()
		svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "demo.json"}, []byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		if len(svcs) != 1 {
			t.Fatalf("%d services", len(svcs))
		}
		return svcs[0]
	}

	svc := ingest(t, model_("models.lex", "lex"))
	if len(svc.Aliases) != 1 || svc.Aliases[0] != "lex" {
		t.Fatalf("a signing name that differs from the endpoint prefix was not "+
			"recorded: prefix %q aliases %v", svc.EndpointPrefix, svc.Aliases)
	}
	if svc.EndpointPrefix != "models.lex" {
		t.Fatalf("the signing name replaced the endpoint prefix: %q", svc.EndpointPrefix)
	}

	// A signing name equal to the prefix says nothing, and an alias that
	// repeats the prefix is noise a reader has to rule out.
	if svc := ingest(t, model_("demo", "demo")); len(svc.Aliases) != 0 {
		t.Errorf("a signing name equal to the endpoint prefix was recorded: %v", svc.Aliases)
	}
	if svc := ingest(t, model_("demo", "")); len(svc.Aliases) != 0 {
		t.Errorf("a service with no sigv4 trait got aliases: %v", svc.Aliases)
	}
}
