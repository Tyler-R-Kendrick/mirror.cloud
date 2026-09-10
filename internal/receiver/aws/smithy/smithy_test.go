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

// bindingDoc carries one member for each placement a Smithy model can state.
const bindingDoc = `{
  "smithy": "2.0",
  "shapes": {
    "com.example#Demo": {
      "type": "service",
      "operations": [{"target": "com.example#Put"}],
      "traits": {
        "aws.protocols#restJson1": {},
        "aws.api#service": {"endpointPrefix": "demo", "sdkId": "Demo"}
      }
    },
    "com.example#Put": {
      "type": "operation",
      "input": {"target": "com.example#PutInput"},
      "output": {"target": "com.example#PutOutput"},
      "traits": {"smithy.api#http": {"method": "PUT", "uri": "/things/{Id}", "code": 200}}
    },
    "com.example#PutInput": {
      "type": "structure",
      "members": {
        "Id":        {"target": "smithy.api#String", "traits": {"smithy.api#httpLabel": {}, "smithy.api#required": {}}},
        "Mode":      {"target": "smithy.api#String", "traits": {"smithy.api#httpQuery": "mode"}},
        "Etag":      {"target": "smithy.api#String", "traits": {"smithy.api#httpHeader": "ETag"}},
        "Meta":      {"target": "smithy.api#String", "traits": {"smithy.api#httpPrefixHeaders": "x-meta-"}},
        "Body":      {"target": "smithy.api#String", "traits": {"smithy.api#httpPayload": {}}},
        "Filters":   {"target": "smithy.api#String", "traits": {"smithy.api#httpQueryParams": {}}},
        "Plain":     {"target": "smithy.api#String"}
      }
    },
    "com.example#PutOutput": {
      "type": "structure",
      "members": {
        "Vpcs":   {"target": "smithy.api#String", "traits": {"smithy.api#xmlName": "vpcSet", "smithy.api#xmlFlattened": {}}},
        "Nested": {"target": "smithy.api#String", "traits": {"smithy.api#jsonName": "nested"}},
        "When":   {"target": "smithy.api#String", "traits": {"smithy.api#timestampFormat": "http-date"}},
        "Attr":   {"target": "smithy.api#String", "traits": {"smithy.api#xmlAttribute": {}}},
        "Code":   {"target": "smithy.api#String", "traits": {"smithy.api#httpResponseCode": {}}},
        "Ns":     {"target": "smithy.api#String", "traits": {"smithy.api#xmlNamespace": {"uri": "http://example/"}}},
        "Items":  {"target": "com.example#ItemList"},
        "Tags":   {"target": "com.example#TagMap"},
        "DryRun": {"target": "smithy.api#String", "traits": {"aws.protocols#ec2QueryName": "DryRun", "smithy.api#xmlName": "dryRun"}}
      }
    },
    "com.example#ItemList": {
      "type": "list",
      "member": {"target": "smithy.api#String", "traits": {"smithy.api#xmlName": "item"}}
    },
    "com.example#TagMap": {
      "type": "map",
      "key":   {"target": "smithy.api#String", "traits": {"smithy.api#xmlName": "k"}},
      "value": {"target": "smithy.api#String", "traits": {"smithy.api#xmlName": "v"}}
    }
  }
}`

// TestIngestRecordsWhereAMemberSitsOnTheWire. `model.MemberBinding` had carried
// these fields since the model was defined and the receiver filled none of
// them, so of the services served exactly one -- from a different receiver --
// carried any. The consumer was already written: `awsquery.unflatten` reads
// Binding.Name to find a member's form field and Binding.XMLFlattened to decide
// whether a list arrives as `Ids.member.1` or `Ids.1`, and with both always
// empty it read every awsQuery and ec2Query request as though no member were
// renamed and no list flattened.
func TestIngestRecordsWhereAMemberSitsOnTheWire(t *testing.T) {
	svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "demo.json"}, []byte(bindingDoc))
	if err != nil {
		t.Fatal(err)
	}
	svc := svcs[0]
	in := svc.Shapes["com.example#PutInput"].Members
	out := svc.Shapes["com.example#PutOutput"].Members

	for _, tc := range []struct {
		member, location, name string
	}{
		{"Id", "label", ""},
		{"Mode", "query", "mode"},
		{"Etag", "header", "ETag"},
		{"Meta", "prefixHeaders", "x-meta-"},
		{"Body", "payload", ""},
		{"Filters", "queryParams", ""},
		{"Plain", "", ""},
	} {
		got := in[tc.member].Binding
		if got.Location != tc.location {
			t.Errorf("%s sits at %q, want %q", tc.member, got.Location, tc.location)
		}
		if got.Name != tc.name {
			t.Errorf("%s is named %q on the wire, want %q", tc.member, got.Name, tc.name)
		}
	}

	// An XML name is what the ec2Query response wants, and the flattening is
	// what its request encoding depends on.
	if got := out["Vpcs"].Binding; got.Name != "vpcSet" || !got.XMLFlattened {
		t.Errorf("Vpcs binding is %+v, want name vpcSet and flattened", got)
	}
	// A list names its own element, and nothing else can: `Vpcs` is `vpcSet`,
	// but the rows inside it are `item`. Reading the element name off the
	// containing member writes `<vpcSet><member>`, which no ec2 client decodes.
	if got := svc.Shapes["com.example#ItemList"].MemberBinding.Name; got != "item" {
		t.Errorf("list element is named %q, want item", got)
	}
	tags := svc.Shapes["com.example#TagMap"]
	if tags.KeyBinding.Name != "k" || tags.MemberBinding.Name != "v" {
		t.Errorf("map entry is named %+v/%+v, want k/v", tags.KeyBinding, tags.MemberBinding)
	}
	// ec2Query asks for a member under a different name than it answers with.
	if got := out["DryRun"].Binding; got.QueryName != "DryRun" || got.Name != "dryRun" {
		t.Errorf("DryRun binding is %+v, want request name DryRun and wire name dryRun", got)
	}
	if got := out["Nested"].Binding.Name; got != "nested" {
		t.Errorf("a jsonName was not recorded: %q", got)
	}
	if got := out["When"].Binding.TimestampFormat; got != "http-date" {
		t.Errorf("timestamp format is %q", got)
	}
	if !out["Attr"].Binding.XMLAttribute {
		t.Error("an xmlAttribute member was not marked")
	}
	if got := out["Code"].Binding.Location; got != "statusCode" {
		t.Errorf("httpResponseCode sits at %q", got)
	}
	if got := out["Ns"].Binding.XMLNamespace; got != "http://example/" {
		t.Errorf("xml namespace is %q", got)
	}
}

// TestARequiredMemberKeepsItsRequirement. The binding is read from the same
// traits as `required`, and the obvious mistake is to replace the member
// rather than add to it.
func TestARequiredMemberKeepsItsRequirement(t *testing.T) {
	svcs, err := (Receiver{}).Ingest(context.Background(), model.SourceRef{Path: "demo.json"}, []byte(bindingDoc))
	if err != nil {
		t.Fatal(err)
	}
	in := svcs[0].Shapes["com.example#PutInput"].Members
	if !in["Id"].Required {
		t.Error("Id is required and no longer says so")
	}
	if in["Mode"].Required {
		t.Error("Mode is not required and says it is")
	}
}
