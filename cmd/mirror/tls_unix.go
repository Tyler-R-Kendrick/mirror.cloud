//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// haveDirLock reports that concurrent starts on one DIR are serialized here.
const haveDirLock = true

// ownedByMe reports whether info belongs to the effective user.
func ownedByMe(info os.FileInfo) (uid int, ok bool) {
	st, isStat := info.Sys().(*syscall.Stat_t)
	if !isStat || st == nil {
		return -1, false
	}
	return int(st.Uid), int(st.Uid) == os.Geteuid()
}

// lockDir holds an exclusive flock on DIR/.lock until unlock is called, so two
// starts on one DIR do not generate over each other. The lock file is opened
// without following a symlink and without blocking on a FIFO.
func lockDir(dir string) (unlock func(), err error) {
	p := filepath.Join(dir, ".lock")
	fd, err := syscall.Open(p, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("--tls-self-signed: lock %s: %w", p, err)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("--tls-self-signed: lock %s is not a regular file", p)
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX); err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("--tls-self-signed: lock %s: %w", p, err)
	}
	return func() {
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		_ = syscall.Close(fd)
	}, nil
}
