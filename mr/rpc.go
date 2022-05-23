package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Action int

const (
	Map Action = iota
	Reduce
	Exit
)

type Report int

const (
	Nothing Report = iota
	MapDone
	ReduceDone
)

type Request struct {
	Id     int
	Report Report
}

type Reply struct {
	Action   Action
	Id       int
	N        int
	Filename string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
