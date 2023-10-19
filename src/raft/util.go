package raft

import (
	"fmt"
	"log"
	"os"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	green := "\033[32m"
	logger := log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds)
	if Debug {
		x := fmt.Sprintf(format, a...)
		logger.Println(green, x)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
