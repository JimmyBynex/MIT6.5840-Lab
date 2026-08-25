package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	waiting map[int]chan opReply
}

type opReply struct {
	err rpc.Err
	rep any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiting:      make(map[int]chan opReply),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	if data := persister.ReadSnapshot(); len(data) > 0 {
		sm.Restore(data)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()

	op := Op{
		Me:  rsm.me,
		Req: req,
	}
	index, preTerm, isLeader := rsm.rf.Start(op)

	result := make(chan opReply, 1)
	rsm.waiting[index] = result

	if !isLeader {
		delete(rsm.waiting, index)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Unlock()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case opReply := <-result:
			rsm.mu.Lock()
			delete(rsm.waiting, index)
			rsm.mu.Unlock()

			return opReply.err, opReply.rep
		//错误的term，不是leader一定超时，但是超时不一定是错误term或者不是leader
		case <-ticker.C:
			curTerm, isLeader := rsm.rf.GetState()
			if !isLeader || curTerm != preTerm {
				rsm.mu.Lock()
				delete(rsm.waiting, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

func (rsm *RSM) reader() {
	for {

		command, ok := <-rsm.applyCh

		//代表被kill了
		if !ok {
			rsm.mu.Lock()
			for _, ch := range rsm.waiting {
				ch <- opReply{err: rpc.ErrWrongLeader}
			}
			rsm.mu.Unlock()
			return
		}

		//普通日志的话
		if command.CommandValid {
			op := command.Command.(Op)
			result := rsm.sm.DoOp(op.Req)
			rsm.mu.Lock()
			ch, ok := rsm.waiting[command.CommandIndex]
			rsm.mu.Unlock()
			if ok && op.Me == rsm.me {
				ch <- opReply{
					err: rpc.OK,
					rep: result,
				}
			}

			//发送给kvserver，回复client后，再进行监测
			//可以考虑异步

			//agent提示不能异步，应为有可能打乱顺序性
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(command.CommandIndex, snapshot)

			}
		}

		if command.SnapshotValid {
			//如果是收到别的服务器发来的snapshot，要使用restore
			rsm.sm.Restore(command.Snapshot)
		}

	}
}
