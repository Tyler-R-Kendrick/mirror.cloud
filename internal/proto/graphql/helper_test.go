package graphql

import "encoding/json"

// jsonBody wraps a query in the envelope a GraphQL server is posted, doing the
// escaping that writing one by hand in a test gets wrong.
func jsonBody(query string) (string, error) {
	b, err := json.Marshal(map[string]any{"query": query})
	return string(b), err
}
