package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Worker's tasks
	// 1. Get a map task, read the file, call the map function and write the intermediate file
	// then use ihash(key) % nReduce to choose the bucket and use JSON to write the intermediate file
	// finished the map task, call the coordinator to get a reduce task

	// 2. Get a reduce task, read the intermediate files, sort the intermediate file by key
	// then call the reduce function and write the result to the output file

	// 3. NoTask/Wait use time.Sleep to wait, then try again

	// 4. If all the tasks are finished, return safely

	// main loop to get tesk and report task
	for {
		reply, ok := requestTask()
		if !ok {
			log.Fatal("cannot get task")
		}
		switch reply.Type {
		// map task
		case TaskMap:
			// create intermediate file and run map function
			intermediate := []KeyValue{}
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			intermediate = append(intermediate, kva...)

			// Pre-create a file and JSON encoder for each reduce task
			files := make([]*os.File, reply.NReduce)
			encs := make([]*json.Encoder, reply.NReduce)
			for r := 0; r < reply.NReduce; r++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.MapTaskId, r)
				f, err := os.Create(oname)
				if err != nil {
					log.Fatalf("cannot create %v", oname)
				}
				files[r] = f
				encs[r] = json.NewEncoder(f)
			}

			// Write intermediate Key/Value pairs to the appropriate reduce task files
			for _, kv := range intermediate {
				r := ihash(kv.Key) % reply.NReduce
				if err := encs[r].Encode(&kv); err != nil {
					log.Fatalf("cannot encode kv to bucket %d: %v", r, err)
				}
			}

			// Close all the files
			for _, f := range files {
				f.Close()
			}

			// Call ReportTask RPC to report the map task is finished successfully to the coordinator
			ok := reportTask(TaskMap, reply.MapTaskId, reply.ReduceIdx, true)
			if !ok {
				log.Fatal("cannot report task")
			}

		// reduce task
		case TaskReduce:
			r := reply.ReduceIdx
			nMap := reply.NMap

			// read all the intermediate files
			kva := make([]KeyValue, 0, 1024)
			for i := 0; i < nMap; i++ {
				fname := fmt.Sprintf("mr-%d-%d", i, r)
				f, err := os.Open(fname)
				if err != nil {
					if os.IsNotExist(err) {
						continue
					}
					log.Fatalf("open %s: %v", fname, err)
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						log.Fatalf("decode %s: %v", fname, err)
					}
					kva = append(kva, kv)
				}
				f.Close()
			}

			// sort by key and call reducef
			sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })

			// write mr-out-r (replace with temporary file atoms -> rename)
			tmpf, err := os.CreateTemp("", fmt.Sprintf("mr-out-%d-*", r))
			if err != nil {
				log.Fatal(err)
			}
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := make([]string, 0, j-i)
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				out := reducef(kva[i].Key, values)
				fmt.Fprintf(tmpf, "%v %v\n", kva[i].Key, out)
				i = j
			}
			tmpf.Close()
			final := fmt.Sprintf("mr-out-%d", r)
			if err := os.Rename(tmpf.Name(), final); err != nil {
				log.Fatal(err)
			}

			// Report the reduce task is finished successfully to the coordinator
			ok := reportTask(TaskReduce, 0, r, true)
			if !ok {
				log.Fatal("cannot report task")
			}

		// wait task
		case TaskWait:
			time.Sleep(200 * time.Millisecond)

		// exit task
		case TaskExit:
			return

		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// Get the task from the coordinator
func requestTask() (AssignTaskReply, bool) {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		return AssignTaskReply{}, false
	}
	return reply, true
}

// Report the task is finished successfully to the coordinator
func reportTask(taskType TaskType, mapTaskId int, reduceIdx int, success bool) bool {
	args := ReportTaskArgs{
		Type:      taskType,
		MapTaskId: mapTaskId,
		ReduceIdx: reduceIdx,
		Success:   success,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	return ok
}

// // Get the file name of a map task that has not yet started
// func GetMapTask() (string, bool) {
// 	args := AssignMapTaskArgs{}
// 	reply := AssignMapTaskReply{}
// 	ok := call("Coordinator.AssignMapTask", &args, &reply)
// 	if !ok || reply.FileName == "" {
// 		return "", false
// 	}
// 	return reply.FileName, true
// }

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
