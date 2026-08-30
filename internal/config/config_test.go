package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFromEnvOverridesDefault(t *testing.T) {
	t.Setenv("MIRROR_BIND", "0.0.0.0:9")
	t.Setenv("MIRROR_ADVERTISE_URL", "https://mirror.example")
	t.Setenv("MIRROR_SEED", "abc")
	t.Setenv("MIRROR_STRICT", "true")
	t.Setenv("MIRROR_PERSIST", "/data")
	t.Setenv("MIRROR_DEFAULT_REGION", "eu-west-1")
	t.Setenv("MIRROR_DEFAULT_ACCOUNT", "123456789012")
	t.Setenv("MIRROR_S3_VALIDATE_PRESIGNED_SIGNATURES", "true")
	t.Setenv("MIRROR_PROXY_MODE", "record")
	t.Setenv("MIRROR_PROXY_ENDPOINT", "https://aws.example")
	c := FromEnv(Default())
	if c.Bind != "0.0.0.0:9" || c.AdvertiseURL != "https://mirror.example" || c.Seed != "abc" || !c.Strict || c.PersistDir != "/data" || c.DefaultRegion != "eu-west-1" || c.DefaultAccount != "123456789012" || !c.S3ValidatePresignedSignatures || c.ProxyMode != "record" || c.ProxyEndpoint != "https://aws.example" {
		t.Fatalf("%+v", c)
	}
}

func TestFromFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "c.json")
	if err := os.WriteFile(p, []byte(`{"bind":"127.0.0.1:1","advertise_url":"https://local","seed":"file","persist":"data","strict":true,"region":"west","account":"1","s3_validate_presigned_signatures":true,"services":["s3"],"tiers":{"s3":"emulate"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	c := FromFile(Default(), p)
	if c.Bind != "127.0.0.1:1" || c.AdvertiseURL != "https://local" || c.Seed != "file" || c.PersistDir != "data" || !c.Strict || c.DefaultRegion != "west" || c.DefaultAccount != "1" || !c.S3ValidatePresignedSignatures || len(c.Services) != 1 || c.Tiers["s3"] != "emulate" {
		t.Fatalf("%+v", c)
	}
	if got := FromFile(c, filepath.Join(dir, "missing")); got.Bind != c.Bind {
		t.Fatal("missing file changed config")
	}
	if invalid := filepath.Join(dir, "invalid"); os.WriteFile(invalid, []byte("{"), 0o644) != nil || FromFile(c, invalid).Bind != c.Bind {
		t.Fatal("invalid file changed config")
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	old, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	t.Setenv("MIRROR_SEED", "env")
	if got := Load(); got.Seed != "env" || got.Bind != Default().Bind {
		t.Fatalf("%+v", got)
	}
}
