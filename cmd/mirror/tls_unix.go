//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// haveDirLock reports that concurrent starts on one DIR are serialized here.
const haveDirLock = true

// maxSelfSignedFile caps what is read from DIR: the files are a few hundred
// bytes, and anything larger is not ours.
const maxSelfSignedFile = 64 << 10

// selfSignedDir is DIR opened once, without following a symlink. Every check,
// read and write after that goes through this descriptor (fstat, openat,
// renameat), so renaming or replacing DIR, or a file in it, after the open
// changes nothing a start reads: it reads the directory it checked, and each
// file is checked on the descriptor it is read from.
type selfSignedDir struct {
	path string
	fd   int
}

func openSelfSignedDir(path string) (*selfSignedDir, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
		return nil, fmt.Errorf("--tls-self-signed: %s is a symbolic link or not a directory; pass the directory itself", path)
	}
	if err != nil {
		return nil, fmt.Errorf("--tls-self-signed: open %s: %w", path, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("--tls-self-signed: %s: %w", path, err)
	}
	if err := dirRule(path, int(st.Uid), int(st.Uid) == os.Geteuid(), os.FileMode(uint32(st.Mode)&0o777)); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &selfSignedDir{path: path, fd: fd}, nil
}

func (d *selfSignedDir) close() { _ = unix.Close(d.fd) }

// lock holds an exclusive flock on DIR/.lock until unlock is called, so two
// starts on one DIR do not generate over each other.
func (d *selfSignedDir) lock() (unlock func(), err error) {
	fd, err := unix.Openat(d.fd, ".lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("--tls-self-signed: lock %s/.lock: %w", d.path, err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || uint32(st.Mode)&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("--tls-self-signed: lock %s/.lock is not a regular file", d.path)
	}
	if err := unix.Flock(fd, unix.LOCK_EX); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("--tls-self-signed: lock %s/.lock: %w", d.path, err)
	}
	return func() {
		_ = unix.Flock(fd, unix.LOCK_UN)
		_ = unix.Close(fd)
	}, nil
}

// read returns name's contents when it passes fileRule on the descriptor it
// is read from. O_NOFOLLOW refuses a symlink and O_NONBLOCK keeps a FIFO from
// blocking the open; a FIFO then fails the regular-file check unread.
func (d *selfSignedDir) read(name string, private bool) ([]byte, bool) {
	fd, err := unix.Openat(d.fd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, false
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || uint32(st.Mode)&unix.S_IFMT != unix.S_IFREG ||
		!fileRule(int(st.Uid) == os.Geteuid(), os.FileMode(uint32(st.Mode)&0o777), private) {
		return nil, false
	}
	b, err := io.ReadAll(io.LimitReader(f, maxSelfSignedFile+1))
	if err != nil || len(b) > maxSelfSignedFile {
		return nil, false
	}
	return b, true
}

// write replaces name with body through a temporary file created exclusively
// in DIR and renamed over it, so a reader sees the old file or the new one and
// a key never exists with a looser mode than it was given.
func (d *selfSignedDir) write(name string, body []byte, mode os.FileMode) error {
	var tmp string
	var fd int
	for i := 0; ; i++ {
		var r [4]byte
		if _, err := rand.Read(r[:]); err != nil {
			return err
		}
		tmp = "." + name + "." + strconv.FormatUint(uint64(binary.BigEndian.Uint32(r[:])), 10)
		var err error
		fd, err = unix.Openat(d.fd, tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EEXIST) || i == 9 {
			return fmt.Errorf("%s/%s: %w", d.path, tmp, err)
		}
	}
	f := os.NewFile(uintptr(fd), tmp)
	_, werr := f.Write(body)
	if werr == nil {
		werr = f.Chmod(mode)
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = unix.Renameat(d.fd, tmp, d.fd, name)
	}
	if werr != nil {
		_ = unix.Unlinkat(d.fd, tmp, 0)
		return fmt.Errorf("%s/%s: %w", d.path, name, werr)
	}
	return nil
}

// removeStaleTemps deletes the temporary files write leaves when a start is
// killed before its rename: regular files named exactly like one, nothing else.
func (d *selfSignedDir) removeStaleTemps() {
	fd, err := unix.Openat(d.fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return
	}
	f := os.NewFile(uintptr(fd), d.path)
	names, _ := f.Readdirnames(-1)
	_ = f.Close()
	for _, n := range names {
		var st unix.Stat_t
		if isStaleTemp(n) && unix.Fstatat(d.fd, n, &st, unix.AT_SYMLINK_NOFOLLOW) == nil &&
			uint32(st.Mode)&unix.S_IFMT == unix.S_IFREG {
			_ = unix.Unlinkat(d.fd, n, 0)
		}
	}
}
