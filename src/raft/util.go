package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	logger := log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds)
	if Debug {
		logger.Printf(format, a...)
	}
}
