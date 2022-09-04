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
		reply.TaskType = FINISH
		return nil
	}
	if c.DoneMap < c.NMap {
		i := c.DoneMap
		for i < c.NMap {
			if c.MapJobs[i] == INIT {
				c.MapJobs[i] = PROCESSING
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
			reply.Filename = c.Files[i]
			c.lock.Unlock()
		}
	} else {
		i := c.DoneReduce
		for i < c.NReduce {
			if c.ReduceJobs[i] == INIT {
				c.ReduceJobs[i] = PROCESSING
				break
			}
			i++
		}
		if i >= c.NReduce {
			reply.TaskType = SCHEDULE
			reply.TaskID = i
			c.lock.Unlock()
			return nil
		} else {
			reply.TaskType = REDUCE
			reply.TaskID = i
			c.lock.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) DoneTask(args *WorkerArgs, reply *WorkerReply) error {
	c.lock.Lock()
	if args.TaskType == MAP {
		c.MapJobs[args.TaskID] = DONE
		c.DoneMap++
	} else if args.TaskType == REDUCE {
		c.ReduceJobs[args.TaskID] = DONE
		c.DoneReduce++
	}
	c.lock.Unlock()
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
	c.lock.Lock()
	// Your code here.
	if c.NReduce == c.DoneReduce {
		ret = true
	}
	c.lock.Unlock()

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
