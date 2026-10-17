//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// This is the fallback for every platform not in tls_unix.go's build list:
// Windows, and unixes such as Solaris, illumos and AIX, which have openat but
// no flock and are simply not built with the descriptor-based code. DIR is
// handled by path. A file must still be a regular file (Lstat; no symlink, no
// FIFO), and the set must pass every certificate check; off Windows, DIR's
// and the files' modes are checked too. Not guaranteed here:
//
//   - that the file checked is the file read: a process able to modify DIR can
//     swap one in between;
//   - who owns anything: neither DIR's owner, the files' owners nor the
//     ancestors' owners are checked (Windows has no POSIX owner, and the others
//     are not wired up), and on Windows no modes are checked at all.
//
// Keep DIR somewhere only you can write. There is no flock either, so two
// starts on one DIR can interleave. That fails closed: each process serves the
// pair it checked or made, and a mismatched set is replaced next time.
const haveDirLock = false

type selfSignedDir struct{ path string }

func openSelfSignedDir(path string) (*selfSignedDir, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("--tls-self-signed: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("--tls-self-signed: %s is a symbolic link or not a directory; pass the directory itself", path)
	}
	if runtime.GOOS != "windows" {
		if err := dirRule(path, -1, true, info.Mode().Perm()); err != nil {
			return nil, err
		}
	}
	return &selfSignedDir{path: path}, nil
}

func (d *selfSignedDir) close() {}

// ownerOf cannot be answered here; ancestors are checked by mode alone.
func ownerOf(os.FileInfo) (uid int, ok bool) { return -1, false }

func (d *selfSignedDir) lock() (unlock func(), err error) { return func() {}, nil }

func (d *selfSignedDir) read(name string, private bool) ([]byte, bool) {
	p := filepath.Join(d.path, name)
	info, err := os.Lstat(p)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return nil, false
	}
	if runtime.GOOS != "windows" && !fileRule(true, info.Mode().Perm(), private) {
		return nil, false
	}
	b, err := os.ReadFile(p)
	return b, err == nil
}

func (d *selfSignedDir) write(name string, body []byte, mode os.FileMode) error {
	return writeAtomic(filepath.Join(d.path, name), body, mode)
}

func (d *selfSignedDir) removeStaleTemps() {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return
	}
	for _, e := range entries {
		if isStaleTemp(e.Name()) && e.Type().IsRegular() {
			_ = os.Remove(filepath.Join(d.path, e.Name()))
		}
	}
}

// writeAtomic writes path through a temporary file in the same directory, so a
// reader sees the old file or the new one and a key never exists with a looser
// mode than it was given.
func writeAtomic(path string, body []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}
