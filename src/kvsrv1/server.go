package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type kvPair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv map[string]*kvPair
}

func MakeKVServer() *KVServer {
	kv := &KVServer{kv: make(map[string]*kvPair)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if existing, ok := kv.kv[args.Key]; ok {
		reply.Value = existing.Value
		reply.Version = existing.Version
		reply.Err = rpc.OK
		return
	} else {
		reply.Err = rpc.ErrNoKey
		return
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Version != 0 {
		if existing, ok := kv.kv[args.Key]; ok {
			if existing.Version == args.Version {
				existing.Value = args.Value
				existing.Version++
				kv.kv[args.Key] = existing
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if _, ok := kv.kv[args.Key]; ok {
			// key exists but client is trying to create with version 0 -> version mismatch
			reply.Err = rpc.ErrVersion
		} else {
			kv.kv[args.Key] = &kvPair{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
