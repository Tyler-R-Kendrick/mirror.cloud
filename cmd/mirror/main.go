// Command mirror is the emulator CLI.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/allservices"
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "up":
		err = cmdUp(os.Args[2:])
	case "env":
		err = cmdEnv(os.Args[2:])
	case "doctor":
		err = cmdDoctor(os.Args[2:])
	case "services":
		err = cmdServices()
	case "spec":
		err = cmdSpec(os.Args[2:])
	case "snapshot":
		err = cmdSnapshot(os.Args[2:])
	case "drift":
		err = cmdDrift(os.Args[2:])
	case "support-matrix":
		err = cmdSupportMatrix(os.Args[2:])
	case "version", "-v", "--version":
		fmt.Println(runtime.Version)
	case "help", "-h", "--help":
		usage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `mirror — generate and serve cloud API emulators

Usage:
  mirror up [services...] [flags]
  mirror env
  mirror doctor
  mirror services
  mirror spec sync|pin|add|update|diff
  mirror snapshot save|load
  mirror drift
  mirror support-matrix
  mirror version

up flags:
  --all                  enable every catalog service (default if none given)
  --profile NAME         aws-core | gcp-core
  --tier ID=mock|emulate|proxy
  --strict               refuse mock-tier (NotImplemented)
  --seed STRING          determinism seed (default mirror)
  --persist DIR          snapshot dir loaded at boot, saved on shutdown
  --bind ADDR            listen address (default 127.0.0.1:4566)
  --advertise-url URL    self-referencing URLs
  --tls-cert PATH --tls-key PATH
  --real-clock           wall clock (default; required for SQS long poll)
`)
}

func cmdUp(args []string) error {
	cfg := config.Load()
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	all := fs.Bool("all", false, "")
	profile := fs.String("profile", "", "")
	strict := fs.Bool("strict", cfg.Strict, "")
	seed := fs.String("seed", cfg.Seed, "")
	persist := fs.String("persist", cfg.PersistDir, "")
	bind := fs.String("bind", cfg.Bind, "")
	adv := fs.String("advertise-url", cfg.AdvertiseURL, "")
	tlsCert := fs.String("tls-cert", cfg.TLSCert, "")
	tlsKey := fs.String("tls-key", cfg.TLSKey, "")
	var tiers []string
	fs.Func("tier", "", func(v string) error { tiers = append(tiers, v); return nil })
	flagArgs, pos := splitFlags(args, map[string]bool{
		"all": true, "strict": true,
	})
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	cfg.Strict = *strict
	cfg.Seed = *seed
	cfg.PersistDir = *persist
	cfg.Bind = *bind
	cfg.AdvertiseURL = *adv
	cfg.TLSCert = *tlsCert
	cfg.TLSKey = *tlsKey
	cfg.LockSHA = lockSHA()
	explicit := len(pos) > 0 || *profile != ""
	cfg.Services = runtime.ExpandServices(pos, *profile, *all || !explicit)
	if cfg.Tiers == nil {
		cfg.Tiers = map[string]model.Tier{}
	}
	for _, t := range tiers {
		k, v, ok := strings.Cut(t, "=")
		if !ok {
			return fmt.Errorf("invalid --tier %q (want id=tier)", t)
		}
		cfg.Tiers[runtime.CanonicalServiceID(k)] = model.Tier(v)
	}

	rt, err := runtime.Boot(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = rt.SavePersist()
		_ = rt.Close()
	}()

	if strings.HasPrefix(cfg.Bind, "0.0.0.0") || strings.HasPrefix(cfg.Bind, "[::]") || strings.HasPrefix(cfg.Bind, ":") {
		fmt.Fprintln(os.Stderr, "WARNING: no authentication. Do not expose this process to untrusted networks. Isolation is network-level and account+region namespacing. An auth token will never be added.")
	}

	addr := cfg.Bind
	if !strings.Contains(addr, ":") {
		addr += ":4566"
	}
	fmt.Fprintf(os.Stderr, "mirror %s listening on http://%s\n", runtime.Version, addr)
	if len(cfg.Services) == 0 {
		fmt.Fprintln(os.Stderr, "services: all")
	} else {
		fmt.Fprintln(os.Stderr, "services:", strings.Join(cfg.Services, ", "))
	}

	srv := &http.Server{Addr: addr, Handler: rt.Handler()}
	if cfg.TLSCert != "" {
		return srv.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
	}
	return srv.ListenAndServe()
}

func cmdEnv(args []string) error {
	fs := flag.NewFlagSet("env", flag.ContinueOnError)
	bind := fs.String("bind", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := config.Load()
	url := cfg.AdvertiseURL
	if *bind != "" {
		url = "http://" + *bind
	}
	if url == "" {
		url = "http://" + cfg.Bind
		if !strings.Contains(cfg.Bind, "://") && !strings.HasPrefix(url, "http") {
			url = "http://" + cfg.Bind
		}
	}
	if !strings.HasPrefix(url, "http") {
		url = "http://" + url
	}
	host := strings.TrimPrefix(strings.TrimPrefix(url, "https://"), "http://")
	fmt.Printf("export AWS_ENDPOINT_URL=%s\n", url)
	fmt.Printf("export AWS_ENDPOINT_URL_S3=%s\n", url)
	fmt.Printf("export AWS_ACCESS_KEY_ID=test\n")
	fmt.Printf("export AWS_SECRET_ACCESS_KEY=test\n")
	fmt.Printf("export AWS_DEFAULT_REGION=%s\n", cfg.DefaultRegion)
	fmt.Printf("export AWS_EC2_METADATA_DISABLED=true\n")
	fmt.Printf("export STORAGE_EMULATOR_HOST=%s\n", host)
	fmt.Printf("export AWS_S3_FORCE_PATH_STYLE=true\n")
	return nil
}

func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	bind := fs.String("bind", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := config.Load()
	addr := cfg.Bind
	if *bind != "" {
		addr = *bind
	}
	findings := 0
	say := func(ok bool, title, fix string) {
		mark := "ok"
		if !ok {
			mark = "FIX"
			findings++
		}
		fmt.Printf("[%s] %s\n", mark, title)
		if !ok && fix != "" {
			fmt.Printf("      %s\n", fix)
		}
	}

	ep := os.Getenv("AWS_ENDPOINT_URL")
	want := "http://" + addr
	say(ep != "" && strings.Contains(ep, strings.TrimPrefix(addr, "127.0.0.1")),
		"AWS_ENDPOINT_URL points at the emulator",
		fmt.Sprintf("export AWS_ENDPOINT_URL=%s", want))
	if v := os.Getenv("AWS_ENDPOINT_URL_S3"); v != "" && !strings.Contains(v, "127.0.0.1") && !strings.Contains(v, "localhost") && !strings.Contains(v, addr) {
		say(false, "AWS_ENDPOINT_URL_S3 looks like the real cloud", "export AWS_ENDPOINT_URL_S3="+want)
	} else {
		say(true, "AWS_ENDPOINT_URL_S3 is unset or local", "")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		say(true, "port "+addr+" is bound (emulator likely running)", "")
	} else {
		_ = ln.Close()
		say(false, "nothing is listening on "+addr, "run: mirror up")
	}

	reg := os.Getenv("AWS_DEFAULT_REGION")
	say(reg == "" || reg == cfg.DefaultRegion,
		"client region matches emulator default "+cfg.DefaultRegion,
		"export AWS_DEFAULT_REGION="+cfg.DefaultRegion)

	say(os.Getenv("AWS_S3_FORCE_PATH_STYLE") == "true" || os.Getenv("AWS_S3_FORCE_PATH_STYLE") == "1",
		"path-style S3 addressing",
		"export AWS_S3_FORCE_PATH_STYLE=true")

	if ep == "" || strings.Contains(ep, "amazonaws.com") {
		say(false, "client may be reaching the real AWS cloud", "export AWS_ENDPOINT_URL="+want)
	} else {
		say(true, "client is not pointed at amazonaws.com", "")
	}

	se := os.Getenv("STORAGE_EMULATOR_HOST")
	host := addr
	say(se != "", "STORAGE_EMULATOR_HOST", "export STORAGE_EMULATOR_HOST="+host)

	lock := "specs/mirror.lock"
	if _, err := os.Stat(lock); err != nil {
		say(false, "spec lock missing (bootstrap catalog in use)", "make specs-sync")
	} else {
		say(true, "spec lock present", "")
	}

	if findings > 0 {
		return fmt.Errorf("%d finding(s)", findings)
	}
	fmt.Println("doctor: clean")
	return nil
}

func cmdServices() error {
	b := catalog.Bundle()
	emu := map[string]bool{}
	for _, f := range registry.Factories() {
		if f.Tier == model.TierEmulate {
			emu[f.ServiceID] = true
		}
	}
	for _, svc := range b.Services {
		tier := "mock"
		if emu[svc.ID] {
			tier = "emulate"
		}
		fmt.Printf("%s\t%s\t%s\t%d ops\n", svc.ID, svc.Protocol, tier, len(svc.Operations))
	}
	return nil
}

func cmdSpec(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mirror spec sync|pin|add|update|diff")
	}
	switch args[0] {
	case "sync", "pin", "update":
		fmt.Println("run: make specs-sync && make generate")
		return nil
	case "add":
		if len(args) < 2 {
			return fmt.Errorf("usage: mirror spec add <service>")
		}
		p := "specs/mirror.set"
		b, _ := os.ReadFile(p)
		line := args[1] + "\n"
		if strings.Contains(string(b), args[1]) {
			fmt.Println("already in specs/mirror.set")
			return nil
		}
		f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.WriteString(line); err != nil {
			return err
		}
		fmt.Printf("appended %s to %s; run make specs-sync && make generate\n", args[1], p)
		return nil
	case "diff":
		fmt.Println("run: go run ./cmd/mirrorgen --diff")
		return nil
	default:
		return fmt.Errorf("unknown spec subcommand %q", args[0])
	}
}

func cmdSnapshot(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mirror snapshot save|load --name NAME [--account ID]")
	}
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	name := fs.String("name", "default", "")
	account := fs.String("account", "", "")
	persist := fs.String("persist", ".mirror/persist", "")
	_ = account
	switch args[0] {
	case "save", "load":
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
	default:
		return fmt.Errorf("usage: mirror snapshot save|load --name NAME")
	}
	dir := filepath.Join(".mirror/snapshots", *name)
	src := filepath.Join(*persist, "state.tar")
	dst := filepath.Join(dir, "state.tar")
	switch args[0] {
	case "save":
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		b, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("no persist state at %s (run with --persist): %w", src, err)
		}
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			return err
		}
		fmt.Println("saved", dst)
	case "load":
		b, err := os.ReadFile(dst)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(*persist, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(src, b, 0o644); err != nil {
			return err
		}
		fmt.Println("loaded", dst, "into", src, "; restart mirror up --persist", *persist)
	}
	return nil
}

func cmdDrift(args []string) error {
	fs := flag.NewFlagSet("drift", flag.ContinueOnError)
	a := fs.String("emulated", "", "path to emulated response body")
	b := fs.String("recorded", "", "path to recorded response body")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *a == "" || *b == "" {
		return fmt.Errorf("usage: mirror drift --emulated FILE --recorded FILE")
	}
	ab, err := os.ReadFile(*a)
	if err != nil {
		return err
	}
	bb, err := os.ReadFile(*b)
	if err != nil {
		return err
	}
	if bytesEqual(ab, bb) {
		fmt.Println("identical")
		return nil
	}
	fmt.Printf("divergence: emulated %d bytes recorded %d bytes sha %s vs %s\n", len(ab), len(bb), shortSHA(ab), shortSHA(bb))
	return fmt.Errorf("diverged")
}

func cmdSupportMatrix(args []string) error {
	fs := flag.NewFlagSet("support-matrix", flag.ContinueOnError)
	out := fs.String("out", "docs/SUPPORT.md", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	body := runtime.SupportMatrix()
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(*out, []byte(body), 0o644); err != nil {
		return err
	}
	fmt.Println("wrote", *out)
	return nil
}

func splitFlags(args []string, boolFlags map[string]bool) (flags, pos []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") {
			pos = append(pos, a)
			continue
		}
		flags = append(flags, a)
		name := strings.TrimLeft(a, "-")
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			continue
		}
		if boolFlags[name] {
			continue
		}
		if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			i++
			flags = append(flags, args[i])
		}
	}
	return flags, pos
}

func lockSHA() string {
	b, err := os.ReadFile("specs/mirror.lock")
	if err != nil {
		return "bootstrap"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func shortSHA(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:8])
}
