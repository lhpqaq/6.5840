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

type Status struct {
	status int
	time   time.Time
}

type Coordinator struct {
	// Your definitions here.
	ReduceTasks map[int]Status
	Files       map[string]Status
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
			if status.status == 0 {
				reply.TaskType = 1 // reduce
				reply.NReduce = c.NReduce
				reply.ReduceId = id
				// c.ReduceTasks[id] = 1
				c.ReduceTasks[id] = Status{
					status: 1,
					time:   time.Now(),
				}
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
			if status.status == 0 {
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
		// c.Files[fileMap].status = 1 // Doing
		c.Files[fileMap] = Status{
			status: 1,
			time:   time.Now(),
		}
		reply.TaskId = c.TaskID
		reply.NReduce = c.NReduce
	}
	c.TaskID++
	return nil
}

func (c *Coordinator) GetNotice(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == 0 {
		c.Files[args.MapFile] = Status{
			status: 2,
		} // Done
	} else {
		c.ReduceTasks[args.ReduceId] = Status{
			status: 2,
		}
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
		for id, status := range c.ReduceTasks {
			if status.status == 1 {
				if time.Since(status.time) >= time.Second*10 {
					c.ReduceTasks[id] = Status{
						status: 0,
					}
				}
			}
		}
	} else {
		for file, status := range c.Files {
			if status.status == 1 {
				if time.Since(status.time) >= time.Second*10 {
					c.Files[file] = Status{
						status: 0,
					}
				}
			}
		}
	}

	if c.MapDone {
		for _, status := range c.ReduceTasks {
			if status.status != 2 {
				return false
			}
		}
		c.AllDone = true
	} else {
		for _, status := range c.Files {
			if status.status != 2 {
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
	c.Files = make(map[string]Status)
	c.ReduceTasks = make(map[int]Status)
	for _, file := range files {
		c.Files[file] = Status{
			status: 0,
			time:   time.Now(),
		}
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Status{
			status: 0,
			time:   time.Now(),
		}
	}
	c.MapDone = false
	c.AllDone = false
	c.TaskID = 1
	// Your code here.
	c.NReduce = nReduce
	c.server()
	return &c
}
