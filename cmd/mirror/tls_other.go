//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package main

import "os"

// haveDirLock is false here: there is no flock, so two starts on one DIR can
// interleave their writes. That fails closed: each process serves the pair it
// checked or made in memory, and a set left mismatched on disk is not reusable
// and is replaced by the next start.
const haveDirLock = false

// ownedByMe cannot be answered here (no POSIX owner); the check is skipped.
func ownedByMe(os.FileInfo) (uid int, ok bool) { return -1, true }

func lockDir(string) (unlock func(), err error) { return func() {}, nil }
