package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
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
	if c.DoneMap < c.NMap {
		i := c.DoneMap
		for i < c.NMap {
			if c.MapJobs[i] == DONE {
				c.DoneMap = i + 1
			} else if c.MapJobs[i] == INIT {
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
			reply.TaskID = i
			reply.Filename = c.Files[i]
			c.lock.Unlock()
			go c.HeartBeat(reply)
			return nil
		}
	} else {
		i := c.DoneReduce
		if i == c.NReduce {
			reply.TaskType = FINISH
			c.lock.Unlock()
			return nil
		}
		for i < c.NReduce {
			if c.ReduceJobs[i] == DONE {
				c.DoneReduce = i + 1
			}
			if c.ReduceJobs[i] == INIT {
				c.ReduceJobs[i] = PROCESSING
				break
			}
			i++
		}
		if i >= c.NReduce {
			reply.TaskType = SCHEDULE
			c.lock.Unlock()
			return nil
		} else {
			reply.TaskType = REDUCE
			reply.TaskID = i
			c.lock.Unlock()
			go c.HeartBeat(reply)
			return nil
		}
	}
	c.lock.Unlock()
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

// HeartBeat If you choose to implement Backup Tasks (Section 3.6),
// note that we test that your code doesn't schedule extraneous tasks when workers execute tasks without crashing.
// Backup tasks should only be scheduled after some relatively long period of time (e.g., 10s).
// Here I implement it by epoll it until lost
func (c *Coordinator) HeartBeat(reply *WorkerReply) error {
	//for {
	//	time.Sleep(4 * time.Second)
	//	ok := call("Worker.IsActive", WorkerArgs{}, nil)
	//	if !ok {
	//		break
	//	}
	//}
	time.Sleep(10 * time.Second)
	c.lock.Lock()
	switch reply.TaskType {
	case MAP:
		if c.MapJobs[reply.TaskID] != DONE {
			c.MapJobs[reply.TaskID] = INIT
			if c.DoneMap > reply.TaskID {
				c.DoneMap = reply.TaskID
			}
			fmt.Printf("reset MAP task %d INIT, DoneMap = %d\n", reply.TaskID, c.DoneMap)
		}
		break
	case REDUCE:
		if c.ReduceJobs[reply.TaskID] != DONE {
			c.ReduceJobs[reply.TaskID] = INIT
			if c.DoneReduce > reply.TaskID {
				c.DoneReduce = reply.TaskID
			}
			fmt.Printf("reset REDUCE task %d INIT, DoneReduce = %d\n", reply.TaskID, c.DoneReduce)
		}
		break
	}
	c.lock.Unlock()
	return nil
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
