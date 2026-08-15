package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state ServerState

	currentTerm int
	votedFor    int
	log         []LogEntry

	lastIncludedIndex int
	lastIncludeTerm   int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionGen int
}

type ServerState int
type LogEntry struct {
	Term    int
	Command interface{}
}

// 长下标（论文/RPC/Start/next/match/commit/lastApplied）= 短下标 + lastIncludedIndex
// log[0] 是 lastIncludedIndex 那条（初始 dummy）。访问 rf.log 必须先减。

// 对于这种软性限制，也就是业务限制的，使用helper函数，其他的感觉没必要
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) sliceIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludeTerm)
	rf.persister.Save(w.Bytes(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludeTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	offset := rf.sliceIndex(index)
	if offset < 0 || offset >= len(rf.log) {
		return
	}
	rf.lastIncludeTerm = rf.log[offset].Term
	rf.lastIncludedIndex = index
	// 新底层数组，丢掉对已裁前缀的引用，便于 GC
	// 使用log = log[offset:]的方法只是把底层指针向前移动，无法实现切片空间的回收
	rf.log = append([]LogEntry(nil), rf.log[offset:]...)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludeTerm)
	rf.persister.Save(w.Bytes(), snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	//为什么reply中需要有term？
	//因为reply中需要有term是为了让candidate知道当前的term是否过期，
	//如果reply中的term大于candidate的term，说明candidate的term过期了，需要更新自己的term，并且变为follower。
	//虽然原来也会持续投票，超时，收到leader的消息变为follower，但是这样会给网络带来太多消息和
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//1. candidate term 落后
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//2. candidate term 领先：降级后继续判断是否投票（不要直接 return false）
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//3. 同 term：未投票或已投给该人，且 log 够新（和下标一律用长坐标）
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	logOk := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if logOk && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionGen += 1
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//1. term 太旧
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//2. 合法 leader 联系：升级 + 重置选举时钟
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.electionGen += 1
	reply.Term = rf.currentTerm
	reply.XLen = -1

	//3. PrevLog 一致性。PreLogIndex 是长下标；已裁掉的 prev 只能靠 InstallSnapshot
	if args.PreLogIndex >= 0 {
		if args.PreLogIndex < rf.lastIncludedIndex {
			reply.Success = false
			reply.XLen = rf.lastLogIndex() + 1
			return
		}
		if args.PreLogIndex > rf.lastLogIndex() {
			reply.Success = false
			reply.XLen = rf.lastLogIndex() + 1
			return
		}
		si := rf.sliceIndex(args.PreLogIndex)
		if rf.log[si].Term != args.PrevLogTerm {
			reply.Success = false
			reply.XTerm = rf.log[si].Term
			j := si - 1
			for j >= 0 && rf.log[j].Term == reply.XTerm {
				j--
			}
			reply.XIndex = j + rf.lastIncludedIndex + 1
			return
		}
	}

	//4. 追加（Entries 为空则只是心跳）
	// Prev 已匹配：从 PreLogIndex+1 起覆盖/追加；冲突则截断后追加
	if len(args.Entries) > 0 {
		idx := rf.sliceIndex(args.PreLogIndex) + 1
		for i, ent := range args.Entries {
			if idx+i < len(rf.log) {
				if rf.log[idx+i].Term != ent.Term {
					rf.log = rf.log[:idx+i]
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				}
			} else {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastIdx := rf.lastLogIndex()
		if lastIdx >= 0 {
			rf.commitIndex = min(args.LeaderCommit, lastIdx)
		}
	}
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {

		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.electionGen += 1

	// 旧 snapshot：term 可能已更新，仍要 persist，但不回退
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		return
	}

	// 本机 log 若已包含该边界且 term 一致，保留后面的尾巴；否则整段换成 dummy
	offset := rf.sliceIndex(args.LastIncludedIndex)
	if offset >= 0 && offset < len(rf.log) && rf.log[offset].Term == args.LastIncludeTerm {
		rf.log = append([]LogEntry(nil), rf.log[offset:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludeTerm}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludeTerm)
	rf.persister.Save(w.Bytes(), args.Data)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	} else {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		index = rf.lastLogIndex()
		rf.persist()
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Lock()
		record := rf.electionGen
		rf.mu.Unlock()
		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		// 代数相同代表超时内没收到有效信号，这里相当于包括两个任务，一个是follower等待超时进入candidate，另一个是新开启一轮投票（在不是leader的前提下）
		if rf.state != Leader && record == rf.electionGen {
			//这些操作幂等，可以一直修改，此状态相当于一直在follower
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			term := rf.currentTerm
			rf.persist()
			rf.mu.Unlock()
			go rf.askForVote(term)
			// 不要 return，ticker 必须一直跑才能重选
			continue
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) askForVote(term int) {
	var tmpMu sync.Mutex
	numGetVoted := 1

	// 不需要 WaitGroup：过半即当选，不必等最慢的 RPC；
	// 晚到的 reply 靠 term/state 检查丢弃。
	for i := range rf.peers {
		//不要使用不等于，这样逻辑更清楚
		if i == rf.me {
			continue
		}
		go func(server int) {

			rf.mu.Lock()
			// 请求前，判断是否已经因为遇到状态修改和进入更新的term
			if rf.state != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			var requestVoteArgs RequestVoteArgs
			requestVoteArgs.Term = term
			requestVoteArgs.CandidateId = rf.me
			requestVoteArgs.LastLogIndex = rf.lastLogIndex()
			requestVoteArgs.LastLogTerm = rf.lastLogTerm()
			rf.mu.Unlock()

			// Call 在锁外
			var requestVoteReply RequestVoteReply
			if !rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply) {
				return
			}

			rf.mu.Lock()

			if requestVoteReply.Term > rf.currentTerm {
				rf.state = Follower
				rf.currentTerm = requestVoteReply.Term
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}
			//响应回写前，遇到更高状态已更改或者进入更高的term
			if rf.state != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			if requestVoteReply.VoteGranted {
				tmpMu.Lock()
				numGetVoted++
				won := numGetVoted > len(rf.peers)/2
				tmpMu.Unlock()
				if won && rf.state == Candidate && rf.currentTerm == term {
					rf.state = Leader
					for j := range rf.peers {
						rf.nextIndex[j] = rf.lastLogIndex() + 1
						rf.matchIndex[j] = 0
					}
					rf.mu.Unlock()
					go rf.sendHeartBeat(term)
					return
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendHeartBeat(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// 不要对整轮 WaitGroup：断线 peer 的 Call 很慢，Wait 会拖死心跳，
		// 在线 follower 会选举超时。每轮直接 go，过期 reply 靠 term/state 丢弃。
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				if rf.state != Leader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				if rf.nextIndex[server] > rf.lastLogIndex()+1 {
					rf.nextIndex[server] = rf.lastLogIndex() + 1
				}

				// prev = nextIndex-1 已经不在本机 log 里发 InstallSnapshot
				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					var args InstallSnapShotArgs
					var reply InstallSnapShotReply
					args.Term = rf.currentTerm
					args.LastIncludedIndex = rf.lastIncludedIndex
					args.LastIncludeTerm = rf.lastIncludeTerm
					args.LeaderId = rf.me
					args.Data = rf.persister.ReadSnapshot()
					rf.mu.Unlock()

					if !rf.sendInstallSnapShot(server, &args, &reply) {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						return
					}

					if rf.state != Leader || rf.currentTerm != term {
						return
					}

					rf.nextIndex[server] = args.LastIncludedIndex + 1
					rf.matchIndex[server] = args.LastIncludedIndex
				} else {
					var args AppendEntriesArgs
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					// nextIndex 已是长下标，不要再加 lastIncludedIndex
					args.PreLogIndex = rf.nextIndex[server] - 1
					if args.PreLogIndex >= rf.lastIncludedIndex && args.PreLogIndex <= rf.lastLogIndex() {
						args.PrevLogTerm = rf.log[rf.sliceIndex(args.PreLogIndex)].Term
					}

					// 拷贝 Entries，避免与 Start 并发 append 共享底层数组
					if rf.nextIndex[server] <= rf.lastLogIndex() {
						ents := rf.log[rf.sliceIndex(rf.nextIndex[server]):]
						args.Entries = make([]LogEntry, len(ents))
						copy(args.Entries, ents)
					}
					rf.mu.Unlock()

					var reply AppendEntriesReply
					if !rf.sendAppendEntries(server, &args, &reply) {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						return
					}
					if rf.state != Leader || rf.currentTerm != term {
						return
					}

					if !reply.Success {
						if rf.nextIndex[server] > 1 {
							if reply.XLen != -1 {
								rf.nextIndex[server] = reply.XLen
							} else {
								j := args.PreLogIndex - 1
								for j >= reply.XIndex {
									si := rf.sliceIndex(j)
									if si < 0 || si >= len(rf.log) {
										break
									}
									if rf.log[si].Term == reply.XTerm {
										break
									}
									j--
								}
								rf.nextIndex[server] = j
							}
							if rf.nextIndex[server] < 1 {
								rf.nextIndex[server] = 1
							}
						}
						return
					}
					// 成功：按本次覆盖到的最后下标更新（长下标）
					rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// 从高到低找最大可提交 N：多数 match>=N 且 log[N].Term==currentTerm
					for j := rf.matchIndex[server]; j > rf.commitIndex; j-- {
						si := rf.sliceIndex(j)
						if si < 0 || si >= len(rf.log) {
							continue
						}
						if rf.log[si].Term != rf.currentTerm {
							continue
						}
						confirm := 1
						for p := range rf.peers {
							if p == rf.me {
								continue
							}
							if rf.matchIndex[p] >= j {
								confirm++
							}
						}
						if confirm > len(rf.peers)/2 {
							rf.commitIndex = j
							break
						}
					}
				}
			}(i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		// 不 apply 下标 0 的 dummy
		for rf.lastApplied < rf.commitIndex {
			if rf.lastApplied < rf.lastIncludedIndex {
				msg := raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.persister.ReadSnapshot(),
					SnapshotIndex: rf.lastIncludedIndex,
					SnapshotTerm:  rf.lastIncludeTerm,
				}
				rf.mu.Unlock()
				applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = rf.lastIncludedIndex
				continue
			}
			rf.lastApplied++
			si := rf.sliceIndex(rf.lastApplied)
			if si <= 0 || si >= len(rf.log) {
				continue
			}
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[si].Command,
			}
			rf.mu.Unlock()
			applyCh <- msg // 锁外发送，避免 channel 阻塞时拖死 Raft
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	// log[0] 为占位 dummy，真实命令从下标 1 开始（测试期望第一条 Start 返回 index=1）
	rf.log = []LogEntry{{Term: 0, Command: nil}}

	// dummy 视为已提交、已 apply，不往 applyCh 送
	rf.commitIndex = 0
	rf.lastApplied = 0

	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier(applyCh)
	return rf
}
