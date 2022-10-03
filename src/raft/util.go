package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		t := time.Now()
		a = append([]interface{}{t.Format("2006-01-02 15:04:05.000")}, a...)
		fmt.Printf("%s\t"+format+"\n", a...)
	}
	return
}
