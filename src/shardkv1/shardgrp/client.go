package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	if len(ck.servers) == 0 {
		return "", 0, rpc.ErrWrongGroup
	}
	var args rpc.GetArgs
	args.Key = key
	//这里是有两个新限制
	//第一个是如果这时候这个group直接被删除，server直接消失了，那len（）==0直接返回
	//第二个是一轮过去还是没找到leader，不一定说明这个group死了，可能是刚好错过了，没关系，外层还会重新读cfg再来一次
	//这样虽然损失了一次，但是避免了死循环
	for i := 0; i < len(ck.servers); i++ {
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		return reply.Value, reply.Version, reply.Err
	}
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	if len(ck.servers) == 0 {
		return rpc.ErrWrongGroup
	}
	var args rpc.PutArgs
	args.Key = key
	args.Value = value
	args.Version = version

	firstCall := true
	for i := 0; i < len(ck.servers); i++ {
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			firstCall = false
			continue
		}

		if reply.Err == rpc.ErrVersion {
			if firstCall {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		return reply.Err
	}
	return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	if len(ck.servers) == 0 {
		return nil, rpc.ErrWrongGroup
	}

	var args shardrpc.FreezeShardArgs
	args.Shard = s
	args.Num = num
	for {
		var reply shardrpc.FreezeShardReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if reply.Err != rpc.OK {
			return nil, reply.Err
		}
		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	if len(ck.servers) == 0 {
		return rpc.ErrWrongGroup
	}
	var args shardrpc.InstallShardArgs
	args.Shard = s
	args.Num = num
	args.State = state
	for {
		var reply shardrpc.InstallShardReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	if len(ck.servers) == 0 {
		return rpc.ErrWrongGroup
	}
	var args shardrpc.DeleteShardArgs
	args.Shard = s
	args.Num = num
	for {
		var reply shardrpc.DeleteShardReply
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		return reply.Err
	}
}
