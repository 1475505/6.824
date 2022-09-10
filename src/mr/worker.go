package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//type TypeWorker struct {
//	Unused string
//}
//
//func MakeWorker() *TypeWorker {
//	w := TypeWorker{}
//	w.server()
//	return &w
//}
//
//func (w TypeWorker) server() {
//	rpc.RegisterName("Worker", w)
//	rpc.HandleHTTP()
//	//l, e := net.Listen("tcp", ":1234")
//	sockname := workerSock()
//	os.Remove(sockname)
//	l, e := net.Listen("unix", sockname)
//	fmt.Printf("starting rpc service: %s\n", sockname)
//	if e != nil {
//		log.Fatal("listen error:", e)
//	}
//	go http.Serve(l, nil)
//}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
//	worker := MakeWorker()
//	worker.Work(mapf, reducef)
//	return
//}

//func (w TypeWorker) Work(mapf func(string, string) []KeyValue,
//	reducef func(string, []string) string) {
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		// send an RPC to the coordinator asking for a task.
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.EmitJob", &args, &reply)
		if !ok {
			fmt.Printf("RPC Error: cannot ask for job!")
		}
		if reply.TaskType != SCHEDULE {
			fmt.Printf("start job [%d]%d..\n", reply.TaskType, reply.TaskID)
		}
		// read that file and call the application function, as in mrsequential.go
		switch reply.TaskType {
		case SCHEDULE:
			time.Sleep(time.Second)
			break
		case MAP:
			doMapJob(mapf, reply)
			break
		case REDUCE:
			doReduceJob(reducef, reply)
			break
		case FINISH:
			return
		default:
			fmt.Printf("worker got unknown task.")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMapJob(mapf func(string, string) []KeyValue, reply WorkerReply) {
	var intermediate []KeyValue
	file, err := os.Open(reply.Filename)
	//fmt.Printf("map %s\n", reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	kva := mapf(reply.Filename, string(content))
	//fmt.Printf("map len:%v\n", len(kva))
	file.Close()
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	buckets := make([][]KeyValue, reply.NReduce)
	for idx := range buckets {
		buckets[idx] = []KeyValue{}
	}
	for _, kva := range intermediate {
		// The map part of your worker can use the ihash(key) function to pick the reduce task for a given key.
		idx := ihash(kva.Key) % reply.NReduce
		buckets[idx] = append(buckets[idx], kva)
		//  fmt.Printf("%v add %v to %v\n", reply.TaskID, kva, idx)
	}
	for idx := range buckets {
		oname := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(idx)
		// To ensure that nobody observes partially written files in the presence of crashes,
		// the MapReduce paper mentions the trick of using a temporary file and atomically renaming it
		// once it is completely written.
		// You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.
		ofile, _ := ioutil.TempFile("", oname+"*")
		// The worker's map task code will need a way to store intermediate key/value pairs in files
		// a way that can be correctly read back during reduce tasks is to use Go's encoding/json package
		enc := json.NewEncoder(ofile)
		for _, kva := range buckets[idx] {
			err := enc.Encode(&kva)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		//fmt.Printf("write %v to %v\n", len(buckets[idx]), oname)
		ofile.Close()
	}
	doneArg := WorkerArgs{MAP, reply.TaskID}
	call("Coordinator.DoneTask", &doneArg, nil)
}

// A reasonable naming convention for intermediate files is mr-X-Y,
// where X is the Map task number, and Y is the reduce task number.

func doReduceJob(reducef func(string, []string) string, reply WorkerReply) {
	var intermediate []KeyValue
	for i := 0; i < reply.NMAP; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)
		file, _ := os.Open(iname)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		//fmt.Printf("%s:%v\n", iname, len(intermediate))
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, _ := ioutil.TempFile("", oname+"*")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()

	doneArg := WorkerArgs{REDUCE, reply.TaskID}
	call("Coordinator.DoneTask", &doneArg, nil)
}

//func (w TypeWorker) IsActive(args *WorkerArgs, reply *WorkerReply) error {
//	return nil
//}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
