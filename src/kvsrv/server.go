package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false
const ExpireTimes = 10

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ReqArg struct {
	key string
	end int
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
		for token, req := range kv.req {
			if req.key == args.Key {
				req.end = -1
				kv.req[token] = req
			}
		}
	} else {
		reply.Value = ""
	}
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// time.Sleep(time.Millisecond)
	for token, req := range kv.req {
		if req.exp > ExpireTimes {
			delete(kv.req, token)
			continue
		}
		if token == args.Token {
			req.exp = 1
		} else {
			req.exp += 1
		}
		kv.req[token] = req
	}
	if _, ok := kv.req[args.Token]; ok {
		end := kv.req[args.Token].end
		if end < 0 {
			fmt.Println("fuck, unreachable!!!")
			reply.Value = kv.data[args.Key]
		} else {
			reply.Value = kv.data[args.Key][:end]
		}
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
	kv.req[args.Token] = ReqArg{args.Key, len(reply.Value), 1}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.req = make(map[int64]ReqArg)
	return kv
}
