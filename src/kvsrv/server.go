package kvsrv

import (
	"log"
	"sync"
)

const Debug = false
const ExpireTimes = 20

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ReqArg struct {
	val string
	exp int
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data map[string]string
	req  map[int64]ReqArg
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
	for token, req := range kv.req {
		if req.exp > ExpireTimes {
			delete(kv.req, token)
			continue
		}
		req.exp += 1
		kv.req[token] = req
	}
	if _, ok := kv.req[args.Token]; ok {
		reply.Value = kv.req[args.Token].val
		return
	}
	oldV, ok := kv.data[args.Key]
	if ok {
		reply.Value = oldV
	} else {
		reply.Value = ""
		kv.data[args.Key] = ""
	}
	kv.data[args.Key] += args.Value
	kv.req[args.Token] = ReqArg{reply.Value, 1}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.req = make(map[int64]ReqArg)
	return kv
}
