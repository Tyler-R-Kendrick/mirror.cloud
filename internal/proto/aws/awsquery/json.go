package awsquery

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
)

// The query protocol has a JSON dialect, and it is a transcoding rather than a
// second encoder.
//
// A client that sends `Accept: application/json` to an SQS query endpoint gets
// the same document it would have got as XML, with elements as object keys --
// `{"ListQueuesResponse":{"ListQueuesResult":{"QueueUrl":[...]}}}`. It is the
// form the AWS SDKs used while SQS was migrating off the query protocol, and
// clients pinned to it are exactly the clients an emulator exists for.
//
// Transcoding the XML is not a shortcut around writing a JSON encoder; it is
// what makes the two dialects answer the same. A separate encoder would be a
// second place for a member name, a flattened list or an xmlName to be wrong,
// and the C30 work was about removing one of those. Here the JSON form cannot
// disagree with the XML form, because it is derived from it.

// WantsJSON reports whether the client asked for the JSON dialect. Only an
// explicit application/json is honoured: `*/*`, which every SDK and curl sends,
// means the default, and the default is XML.
func WantsJSON(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept"), ",") {
		if media, _, _ := strings.Cut(part, ";"); strings.EqualFold(strings.TrimSpace(media), "application/json") {
			return true
		}
	}
	return false
}

// xmlToJSON rewrites one query-protocol XML document as the JSON dialect.
//
// Repetition is the only thing XML says and JSON cannot: `<QueueUrl>a</>
// <QueueUrl>b</>` is a list and a lone `<QueueUrl>a</>` is not, and nothing in
// the document distinguishes a one-element list from a scalar. AWS resolves it
// the same way -- a repeated element becomes an array and a single one does
// not -- so a client reading the JSON dialect has to handle both, and does.
func xmlToJSON(document string) ([]byte, error) {
	decoder := xml.NewDecoder(strings.NewReader(document))
	var root any
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		value, err := jsonElement(decoder)
		if err != nil {
			return nil, err
		}
		root = map[string]any{start.Name.Local: value}
		break
	}
	return json.Marshal(root)
}

// jsonElement reads one element's content, having already consumed its start
// tag, and returns the string it contains or the object its children form.
func jsonElement(decoder *xml.Decoder) (any, error) {
	var text strings.Builder
	children := map[string]any{}
	order := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		switch t := token.(type) {
		case xml.CharData:
			text.Write(t)
		case xml.StartElement:
			child, err := jsonElement(decoder)
			if err != nil {
				return nil, err
			}
			order++
			addChild(children, t.Name.Local, child)
		case xml.EndElement:
			if order == 0 {
				// A leaf, including an empty one: `<Value></Value>` and
				// `<Value/>` are the same element and both are "".
				return strings.TrimSpace(text.String()), nil
			}
			return children, nil
		}
	}
}

// addChild records one child under its element name, promoting to a list the
// moment a second child shares that name.
func addChild(into map[string]any, name string, value any) {
	existing, seen := into[name]
	if !seen {
		into[name] = value
		return
	}
	if list, ok := existing.([]any); ok {
		into[name] = append(list, value)
		return
	}
	into[name] = []any{existing, value}
}
