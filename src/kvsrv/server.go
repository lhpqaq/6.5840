package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.data[args.Key]
	if ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
	// for k, v := range kv.data {
	// 	fmt.Println(k, " -:- ", v)
	// }
	// fmt.Println("-----------------")
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.data[args.Key]
	if ok {
		reply.Value = ""
	} else {
		reply.Value = ""
	}
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldV, ok := kv.data[args.Key]
	if ok {
		reply.Value = oldV
	} else {
		reply.Value = ""
		kv.data[args.Key] = ""
	}
	kv.data[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	return kv
}
