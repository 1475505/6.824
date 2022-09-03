package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	INIT       = 0
	PROCESSING = 1
	DONE       = 2
)

type Coordinator struct {
	// Your definitions here.
	NMap       int
	NReduce    int
	DoneMap    int
	DoneReduce int
	MapJobs    []int
	ReduceJobs []int
	Files      []string
	lock       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// respond with the file name of an as-yet-unstarted task.
func (c *Coordinator) EmitJob(args *WorkerArgs, reply *WorkerReply) error {
	//  The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data.
	reply.NMAP = c.NMap
	reply.NReduce = c.NReduce
	c.lock.Lock()
	if c.DoneReduce == c.NReduce {
		reply.TaskType = UNDEFINED
		return nil
	}
	if c.DoneMap < c.NMap {
		i := c.DoneMap
		for i < c.NMap {
			if c.MapJobs[i] != 0 {
				c.MapJobs[i] = 1
				break
			}
			i++
		}
		if i >= c.NMap {
			reply.TaskType = SCHEDULE
			c.lock.Unlock()
			return nil
		} else {
			reply.TaskType = MAP
			c.lock.Unlock()
			reply.Filename = c.Files[i]
		}
	} else {
		i := c.DoneReduce
		for i < c.NReduce {
			if c.ReduceJobs[i] != 0 {
				c.ReduceJobs[i] = 1
				break
			}
			i++
		}
		if i == c.NReduce {
			reply.TaskType = SCHEDULE
			reply.TaskID = i
			c.lock.Unlock()
			return nil
		} else {
			reply.TaskType = MAP
			reply.TaskID = i
			c.lock.Unlock()
			reply.Filename = c.Files[i]
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NMap = len(files)
	c.MapJobs = make([]int, c.NMap)
	c.NReduce = nReduce
	c.ReduceJobs = make([]int, c.NReduce)
	c.server()
	return &c
}
