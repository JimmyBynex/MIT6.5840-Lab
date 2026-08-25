package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	table map[string]entry
}

type entry struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req.(type) {
	case rpc.GetArgs:
		r := req.(rpc.GetArgs)
		kv.mu.Lock()
		if entry, ok := kv.table[r.Key]; ok {
			kv.mu.Unlock()
			return rpc.GetReply{
				Value:   entry.Value,
				Version: entry.Version,
				Err:     rpc.OK,
			}
		}
		kv.mu.Unlock()
		return rpc.GetReply{Err: rpc.ErrNoKey}
	case rpc.PutArgs:
		r := req.(rpc.PutArgs)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		e, ok := kv.table[r.Key]
		if !ok {
			if r.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}

			kv.table[r.Key] = entry{Value: r.Value, Version: r.Version + 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		if e.Version == r.Version {
			kv.table[r.Key] = entry{r.Value, r.Version + 1}
			return rpc.PutReply{Err: rpc.OK}
		}

		return rpc.PutReply{Err: rpc.ErrVersion}

	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	kv.mu.Lock()
	e.Encode(kv.table)
	kv.mu.Unlock()
	return b.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	e := labgob.NewDecoder(bytes.NewBuffer(data))
	t := make(map[string]entry)
	e.Decode(&t)
	kv.mu.Lock()
	kv.table = t
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, table: make(map[string]entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
