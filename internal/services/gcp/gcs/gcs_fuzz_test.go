package gcs

import "testing"

func FuzzParsePath(f *testing.F) {
	f.Add("/storage/v1/b/bucket/o/key")
	f.Add("/upload/storage/v1/b/b/o")
	f.Add("/b/x/o/a%2Fb")
	f.Add("")
	f.Fuzz(func(t *testing.T, path string) {
		_, _ = parsePath(path)
	})
}
