package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type grpStatus int

const (
	Absent  grpStatus = 0
	Serving grpStatus = 1
	Flozen  grpStatus = 2
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu     sync.Mutex
	table  map[string]entry
	status []grpStatus
	nums   []shardcfg.Tnum
}

type entry struct {
	Value   string
	Version rpc.Tversion
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req.(type) {
	case rpc.GetArgs:
		r := req.(rpc.GetArgs)
		kv.mu.Lock()

		if kv.status[shardcfg.Key2Shard(r.Key)] != Serving {
			kv.mu.Unlock()
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

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
		if kv.status[shardcfg.Key2Shard(r.Key)] != Serving {

			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

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

	case shardrpc.FreezeShardArgs:
		r := req.(shardrpc.FreezeShardArgs)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		var rep shardrpc.FreezeShardReply
		rep.Num = kv.nums[r.Shard]
		if r.Num < kv.nums[r.Shard] {
			rep.Err = rpc.ErrStaleNum
			return rep
		}
		if r.Num == kv.nums[r.Shard] && kv.status[r.Shard] != Flozen {
			rep.Err = rpc.ErrStaleNum
			return rep
		}

		kv.nums[r.Shard] = r.Num
		kv.status[r.Shard] = Flozen

		rep.Err = rpc.OK
		shardTable := make(map[string]entry)
		for k, e := range kv.table {
			if shardcfg.Key2Shard(k) != r.Shard {
				continue
			}
			shardTable[k] = e
		}
		b := new(bytes.Buffer)
		e := labgob.NewEncoder(b)
		e.Encode(shardTable)
		rep.State = b.Bytes()
		return rep

	case shardrpc.InstallShardArgs:
		r := req.(shardrpc.InstallShardArgs)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		var rep shardrpc.InstallShardReply

		if kv.nums[r.Shard] > r.Num {
			rep.Err = rpc.ErrStaleNum
			return rep
		}
		if kv.nums[r.Shard] == r.Num {
			if kv.status[r.Shard] != Serving {
				rep.Err = rpc.ErrStaleNum
				return rep
			}
			rep.Err = rpc.OK
			return rep
		}
		kv.nums[r.Shard] = r.Num
		kv.status[r.Shard] = Serving

		rep.Err = rpc.OK
		d := labgob.NewDecoder(bytes.NewBuffer(r.State))
		t := make(map[string]entry)
		d.Decode(&t)
		for k, e := range t {
			kv.table[k] = e
		}
		return rep
	case shardrpc.DeleteShardArgs:
		r := req.(shardrpc.DeleteShardArgs)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		var rep shardrpc.DeleteShardReply

		if r.Num < kv.nums[r.Shard] {
			rep.Err = rpc.ErrStaleNum
			return rep
		}
		if r.Num == kv.nums[r.Shard] && kv.status[r.Shard] == Absent {
			rep.Err = rpc.OK
			return rep
		}
		if r.Num == kv.nums[r.Shard] && kv.status[r.Shard] == Serving {
			rep.Err = rpc.ErrStaleNum
			return rep
		}
		kv.nums[r.Shard] = r.Num
		kv.status[r.Shard] = Absent

		rep.Err = rpc.OK
		for k := range kv.table {
			if shardcfg.Key2Shard(k) != r.Shard {
				continue
			}
			delete(kv.table, k)
		}
		return rep
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
	e.Encode(kv.status)
	e.Encode(kv.nums)
	kv.mu.Unlock()
	return b.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	e := labgob.NewDecoder(bytes.NewBuffer(data))
	t := make(map[string]entry)
	s := make([]grpStatus, shardcfg.NShards)
	n := make([]shardcfg.Tnum, shardcfg.NShards)
	e.Decode(&t)
	e.Decode(&s)
	e.Decode(&n)
	kv.mu.Lock()
	kv.table = t
	kv.status = s
	kv.nums = n
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here

	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here

	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = rep.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {

	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}

	// Your code here

	kv.table = make(map[string]entry)
	kv.status = make([]grpStatus, shardcfg.NShards)
	if gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.status[i] = Serving
		}
	} else {
		for i := range shardcfg.NShards {
			kv.status[i] = Absent
		}
	}
	kv.nums = make([]shardcfg.Tnum, shardcfg.NShards)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
