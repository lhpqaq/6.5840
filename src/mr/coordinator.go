package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Tasks      map[int]int
	Files      []string
	TaskID     int
	NReduce    int
	ReduceTask int
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
	if len(c.Files) == 0 {
		// TODO: 加锁？
		if c.ReduceTask < c.NReduce {
			reply.TaskType = 1 // reduce
			reply.NReduce = c.NReduce
			reply.ReduceId = c.ReduceTask
			c.ReduceTask += 1
		}
	} else {
		reply.TaskType = 0 // map
		reply.FileName = c.Files[0]
		reply.TaskId = c.TaskID
		c.Files = c.Files[1:]
		reply.NReduce = c.NReduce
	}
	c.TaskID++
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Files = files
	c.TaskID = 1
	// Your code here.
	c.NReduce = nReduce
	c.ReduceTask = 0
	c.server()
	return &c
}
