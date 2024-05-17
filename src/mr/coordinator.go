package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	ReduceTasks map[int]int
	Files       map[string]int
	MapDone     bool
	AllDone     bool
	TaskID      int
	NReduce     int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.MapDone {
		for id, status := range c.ReduceTasks {
			if status == 0 {
				reply.TaskType = 1 // reduce
				reply.NReduce = c.NReduce
				reply.ReduceId = id
				c.ReduceTasks[id] = 1
				c.TaskID++
				return nil
			}
		}
		if c.AllDone {
			reply.TaskType = -2
		} else {
			reply.TaskType = -1
		}
	} else {
		var fileMap string
		findMapFile := false
		for file, status := range c.Files {
			if status == 0 {
				fileMap = file
				findMapFile = true
			}
		}
		if !findMapFile {
			reply.TaskType = -1 // Mapping
			return nil
		}
		reply.TaskType = 0 // map
		reply.FileName = fileMap
		c.Files[fileMap] = 1 // Doing
		reply.TaskId = c.TaskID
		reply.NReduce = c.NReduce
	}
	c.TaskID++
	return nil
}

func (c *Coordinator) GetNotice(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.X == 0 {
		c.Files[args.MapFile] = 2 // Done
	} else {
		c.ReduceTasks[args.ReduceId] = 2
	}
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
	// ret := true
	ret := false
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.MapDone {
		for _, status := range c.ReduceTasks {
			if status != 2 {
				return false
			}
		}
		c.AllDone = true
	} else {
		for _, status := range c.Files {
			if status != 2 {
				return false
			}
		}
		c.MapDone = true
	}
	ret = c.AllDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Files = make(map[string]int)
	c.ReduceTasks = make(map[int]int)
	for _, file := range files {
		c.Files[file] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = 0
	}
	c.MapDone = false
	c.AllDone = false
	c.TaskID = 1
	// Your code here.
	c.NReduce = nReduce
	c.server()
	return &c
}
