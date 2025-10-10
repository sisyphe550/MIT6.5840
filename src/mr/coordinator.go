package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskState int

const (
	stateno taskState = iota
	idle
	inProgress
	completed
)

type mapTask struct {
	fileName  string
	mapTaskId int
	state     taskState
	startTime int64
}

type reduceTask struct {
	reduceTaskId int
	state        taskState
	startTime    int64
}

type Coordinator struct {
	// Your definitions here.
	mapTasks        []*mapTask    //Map tasks
	reduceTasks     []*reduceTask //Reduce tasks
	nReduce         int           //Number of reduce tasks
	stage           string        //Map or Reduce
	mutex           sync.Mutex    //Mutex for the coordinator
	finishedMaps    int           //Finished map tasks count
	finishedReduces int           //Finished reduce tasks count
}

// Your code here -- RPC handlers for the worker to call.

// Coordinator's tasks
// Maintaining task queues and status
// Timeout reallocation
// Phase Switching
// Supply RPCs
// Finish Done() so that the coordinator can exit

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch c.stage {
	case "Map":
		// reclaim timeout map tasks
		now := time.Now().Unix()
		for _, mt := range c.mapTasks {
			if mt != nil && mt.state == inProgress && mt.startTime > 0 && now-mt.startTime > 10 {
				mt.state = idle
				mt.startTime = 0
			}
		}
		// allocate idle map tasks
		for _, mt := range c.mapTasks {
			if mt != nil && mt.state == idle {
				mt.state = inProgress
				mt.startTime = now
				reply.Type = TaskMap
				reply.FileName = mt.fileName
				reply.MapTaskId = mt.mapTaskId
				reply.NReduce = c.nReduce
				return nil
			}
		}
		// no task that can be assigned
		reply.Type = TaskWait
		return nil

	case "Reduce":
		// reclaim timeout reduce tasks
		now := time.Now().Unix()
		for _, rt := range c.reduceTasks {
			if rt != nil && rt.state == inProgress && rt.startTime > 0 && now-rt.startTime > 10 {
				rt.state = idle
				rt.startTime = 0
			}
		}
		// allocate idle reduce tasks
		for _, rt := range c.reduceTasks {
			if rt != nil && rt.state == idle {
				rt.state = inProgress
				rt.startTime = now
				reply.Type = TaskReduce
				reply.ReduceIdx = rt.reduceTaskId
				reply.NMap = len(c.mapTasks)
				return nil
			}
		}
		// no task that can be assigned
		reply.Type = TaskWait
		return nil

	default:
		reply.Type = TaskExit
		return nil
	}
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.Type {
	case TaskMap:
		if args.Success {
			if args.MapTaskId >= 0 && args.MapTaskId < len(c.mapTasks) {
				mt := c.mapTasks[args.MapTaskId]
				if mt != nil && mt.state != completed {
					mt.state = completed
					mt.startTime = 0
					c.finishedMaps++
				}
			}
			if c.stage == "Map" && c.finishedMaps == len(c.mapTasks) {
				// switch to the Reduce phase and initialize the reduce task
				c.stage = "Reduce"
				// c.reduceTasks = make([]*reduceTask, c.nReduce)
				for i := 0; i < c.nReduce; i++ {
					c.reduceTasks[i] = &reduceTask{
						reduceTaskId: i,
						state:        idle,
						startTime:    0,
					}
				}
			}
		}
		return nil

	case TaskReduce:
		if args.Success {
			if args.ReduceIdx >= 0 && args.ReduceIdx < len(c.reduceTasks) {
				rt := c.reduceTasks[args.ReduceIdx]
				if rt != nil && rt.state != completed {
					rt.state = completed
					rt.startTime = 0
					c.finishedReduces++
				}
			}
			if c.finishedReduces == c.nReduce {
				c.stage = "Done"
			}
		}
		return nil

	case TaskWait:
		return nil

	case TaskExit:
		return nil

	default:
		return nil
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.stage == "Done" {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make([]*mapTask, len(files))
	c.reduceTasks = make([]*reduceTask, nReduce)

	// Initialize the coordinator file list that not be assigned
	for mapTaskId, file := range files {
		c.mapTasks[mapTaskId] = &mapTask{
			fileName:  file,
			mapTaskId: mapTaskId,
			state:     idle,
			startTime: 0,
		}
	}
	// Initialize the number of reduce tasks
	c.nReduce = nReduce
	c.stage = "Map"

	// start listen the RPC from the worker
	c.server()
	return &c
}
