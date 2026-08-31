// Package config loads flags, env, and file with precedence
// flags > env > file > default.
package config

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Config is process configuration.
type Config struct {
	Bind                          string
	AdvertiseURL                  string
	Seed                          string
	PersistDir                    string
	Strict                        bool
	Services                      []string
	Tiers                         map[string]model.Tier
	DefaultRegion                 string
	DefaultAccount                string
	S3ValidatePresignedSignatures bool
	TLSCert                       string
	TLSKey                        string
	ProxyMode                     string
	ProxyEndpoint                 string
	CassetteDir                   string
	LockSHA                       string
}

// Default returns the documented defaults.
func Default() Config {
	return Config{
		Bind:           "127.0.0.1:4566",
		Seed:           "mirror",
		DefaultRegion:  "us-east-1",
		DefaultAccount: "000000000000",
		Tiers:          map[string]model.Tier{},
		ProxyMode:      "off",
		CassetteDir:    ".mirror/cassettes",
	}
}

// FromEnv overlays environment variables.
func FromEnv(c Config) Config {
	if v := os.Getenv("MIRROR_BIND"); v != "" {
		c.Bind = v
	}
	if v := os.Getenv("MIRROR_ADVERTISE_URL"); v != "" {
		c.AdvertiseURL = v
	}
	if v := os.Getenv("MIRROR_SEED"); v != "" {
		c.Seed = v
	}
	if v := os.Getenv("MIRROR_STRICT"); v == "1" || strings.EqualFold(v, "true") {
		c.Strict = true
	}
	if v := os.Getenv("MIRROR_PERSIST"); v != "" {
		c.PersistDir = v
	}
	if v := os.Getenv("MIRROR_DEFAULT_REGION"); v != "" {
		c.DefaultRegion = v
	}
	if v := os.Getenv("MIRROR_DEFAULT_ACCOUNT"); v != "" {
		c.DefaultAccount = v
	}
	if v := os.Getenv("MIRROR_S3_VALIDATE_PRESIGNED_SIGNATURES"); v == "1" || strings.EqualFold(v, "true") {
		c.S3ValidatePresignedSignatures = true
	}
	if v := os.Getenv("MIRROR_PROXY_MODE"); v != "" {
		c.ProxyMode = v
	}
	if v := os.Getenv("MIRROR_PROXY_ENDPOINT"); v != "" {
		c.ProxyEndpoint = v
	}
	return c
}

// Load applies file then env over defaults. Flags overlay in the CLI.
func Load() Config {
	c := Default()
	c = FromFile(c, ".mirror/config.json")
	return FromEnv(c)
}

// FromFile overlays a JSON config file when present.
func FromFile(c Config, path string) Config {
	b, err := os.ReadFile(path)
	if err != nil {
		return c
	}
	var f struct {
		Bind                          string            `json:"bind"`
		AdvertiseURL                  string            `json:"advertise_url"`
		Seed                          string            `json:"seed"`
		PersistDir                    string            `json:"persist"`
		Strict                        bool              `json:"strict"`
		DefaultRegion                 string            `json:"region"`
		DefaultAccount                string            `json:"account"`
		S3ValidatePresignedSignatures bool              `json:"s3_validate_presigned_signatures"`
		Services                      []string          `json:"services"`
		Tiers                         map[string]string `json:"tiers"`
	}
	if json.Unmarshal(b, &f) != nil {
		return c
	}
	if f.Bind != "" {
		c.Bind = f.Bind
	}
	if f.AdvertiseURL != "" {
		c.AdvertiseURL = f.AdvertiseURL
	}
	if f.Seed != "" {
		c.Seed = f.Seed
	}
	if f.PersistDir != "" {
		c.PersistDir = f.PersistDir
	}
	if f.Strict {
		c.Strict = true
	}
	if f.DefaultRegion != "" {
		c.DefaultRegion = f.DefaultRegion
	}
	if f.DefaultAccount != "" {
		c.DefaultAccount = f.DefaultAccount
	}
	if f.S3ValidatePresignedSignatures {
		c.S3ValidatePresignedSignatures = true
	}
	if len(f.Services) > 0 {
		c.Services = f.Services
	}
	if len(f.Tiers) > 0 {
		if c.Tiers == nil {
			c.Tiers = map[string]model.Tier{}
		}
		for k, v := range f.Tiers {
			c.Tiers[k] = model.Tier(v)
		}
	}
	return c
}
