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
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

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
	fmt.Printf("I am a worker\n")
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	reply, err := CallForTask()
	if err != nil {
		log.Fatalf("call failed!\n")
	}
	if reply.TaskType == 0 {
		DoMap(reply, mapf)
	} else if reply.TaskType == 1 {
		// DoReduce(reply.FileName, reducef)
		fmt.Println("reduce")
	}

}

func DoReduce(reply WorkerReply, reducef func(string, []string) string) {
	kva, err := ReadKeyValueFromFile(reply.FileName)
	if err != nil {
		log.Fatalf("error reading key-value pairs from file: %v", err)
	}
	fmt.Print(kva)

}

func DoMap(reply WorkerReply, mapf func(string, string) []KeyValue) string {
	contents, err := ioutil.ReadFile(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	kva := mapf(reply.FileName, string(contents))
	outputFileName := fmt.Sprintf("mr-map-%d", reply.TaskId)
	if err := writeKeyValueToFile(outputFileName, kva); err != nil {
		log.Fatalf("error writing key-value pairs to file: %v", err)
	}
	return outputFileName
}

func CallForTask() (WorkerReply, error) {
	args := WorkerArgs{}

	reply := WorkerReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.TaskType %v\n", reply.TaskType)
		if reply.TaskType == 0 {
			fmt.Printf("reply.FileName %v\n", reply.FileName)
		} else {
			fmt.Printf("reply.TaskType %v\n", reply.TaskType)
		}
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply, errors.New("call failed")
}

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

func writeKeyValueToFile(fileName string, kva []KeyValue) error {
	outputFile, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("cannot create %s: %v", fileName, err)
	}
	defer outputFile.Close()
	enc := json.NewEncoder(outputFile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			return fmt.Errorf("error encoding key-value pair: %v", err)
		}
	}

	return nil
}

func ReadKeyValueFromFile(fileName string) ([]KeyValue, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %v", fileName, err)
	}
	defer file.Close()
	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("error decoding key-value pair: %v", err)
		}
		kva = append(kva, kv)
	}

	return kva, nil
}
