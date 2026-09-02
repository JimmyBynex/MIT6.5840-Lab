package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	cfgName  string
	nextName string
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, cfgName: "kun", nextName: "kun-next"}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	nextStr, _, err := sck.IKVClerk.Get(sck.nextName)
	if err != rpc.OK {
		return
	}
	next := shardcfg.FromString(nextStr)
	cur := sck.Query()
	if next.Num > cur.Num {
		sck.ChangeConfigTo(next)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	cfgString := cfg.String()
	sck.IKVClerk.Put(sck.cfgName, cfgString, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	oldStr, version, _ := sck.IKVClerk.Get(sck.cfgName)
	old := shardcfg.FromString(oldStr)
	if new.Num <= old.Num {
		return
	}

	nextStr, nver, nerr := sck.IKVClerk.Get(sck.nextName)
	claimed := false
	if nerr == rpc.OK {
		next := shardcfg.FromString(nextStr)
		if next.Num > old.Num {
			if next.String() != new.String() {
				return
			}
			claimed = true
		}
	} else if nerr == rpc.ErrNoKey {
		nver = 0
	} else {
		return
	}
	if !claimed {
		err := sck.IKVClerk.Put(sck.nextName, new.String(), nver)
		if err != rpc.OK {
			got, _, gerr := sck.IKVClerk.Get(sck.nextName)
			if gerr != rpc.OK || shardcfg.FromString(got).String() != new.String() {
				return
			}
		}
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		failed bool
	)
	for s, g := range old.Shards {
		if new.Shards[s] == g {
			continue
		}
		wg.Add(1)
		go func(s int, g tester.Tgid) {
			defer wg.Done()
			oldClerk := shardgrp.MakeClerk(sck.clnt, old.Groups[g])
			state, err := oldClerk.FreezeShard(shardcfg.Tshid(s), new.Num)
			if err != rpc.OK {
				mu.Lock()
				failed = true
				mu.Unlock()
				return
			}
			newClerk := shardgrp.MakeClerk(sck.clnt, new.Groups[new.Shards[s]])
			if err := newClerk.InstallShard(shardcfg.Tshid(s), state, new.Num); err != rpc.OK {
				mu.Lock()
				failed = true
				mu.Unlock()
			}
		}(s, g)
	}
	wg.Wait()
	if failed {
		return
	}

	for {
		err := sck.IKVClerk.Put(sck.cfgName, new.String(), version)
		if err == rpc.OK {
			break
		}
		curStr, ver, qerr := sck.IKVClerk.Get(sck.cfgName)
		if qerr != rpc.OK {
			continue
		}
		cur := shardcfg.FromString(curStr)
		if cur.Num > new.Num {
			return
		}
		if cur.Num == new.Num {
			break
		}
		version = ver
	}

	for s, g := range old.Shards {
		if new.Shards[s] == g {
			continue
		}
		wg.Add(1)
		go func(s int, g tester.Tgid) {
			defer wg.Done()
			oldClerk := shardgrp.MakeClerk(sck.clnt, old.Groups[g])
			oldClerk.DeleteShard(shardcfg.Tshid(s), new.Num)
		}(s, g)
	}
	wg.Wait()
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfgString, _, _ := sck.IKVClerk.Get(sck.cfgName)
	return shardcfg.FromString(cfgString)
}
