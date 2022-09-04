package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	// SCHEDULE e.g. reduces can't start until the last map has finished.
	//One possibility is for workers :
	// -> to periodically ask the coordinator for work, sleeping with time.Sleep() between each request.
	//Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits:
	//either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own thread,
	//so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
	SCHEDULE  = 0 //Workers will sometimes need to wait.
	MAP       = 1
	REDUCE    = 2
	UNDEFINED = 3
	FINISH    = 4
)

// Add your RPC definitions here.
type WorkerArgs struct {
	TaskType int
	TaskID   int
}

type WorkerReply struct {
	TaskType int
	Filename string
	TaskID   int
	NMAP     int
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
