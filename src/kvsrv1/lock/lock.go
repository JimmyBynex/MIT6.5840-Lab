package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"github.com/google/uuid"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l string // lock key in the KV store
	//把uuid提前到实例化，节省开销和明确逻辑
	id string // fixed owner identity for this Lock instance
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
//
// Lock state convention:
//   - free: key missing (ErrNoKey) or value == ""
//   - held: value == owner id
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	return &Lock{
		ck: ck,
		l:  l,
		id: uuid.New().String(),
	}
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.l)

		//重试发现锁已经被自己持有了，直接返回

		// 为什么需要这个判断，其实我一开始觉得是冗余的，因为我认为在处理errMay的时候已经处理干净了
		// 但是有一个case就是在clientA出现ErrMay的时候，如果clientA一开始put没有成功到达，但是clientA的后续判断get到达了
		// 这里有两种可能，一种是检测clientA并没有写入成功但是也没有被别的client持有，另一种是被clientB持有锁
		// 如果是并没有持有锁也就是put失败，那很自然进入下一个循环
		// 可是如果是被clientB持有锁之后，在我进入下一个循环判断之前释放锁，刚好那个时候clientA之前没有到达的put请求到达server，server上锁
		// 这时候如果我不加上这个判断，clientA认为有锁被别的client持有，陷入死锁
		if err == rpc.OK && value == lk.id {
			return
		}

		// 只有 free 才继续往下 CAS。
		// free ≡ ErrNoKey（key 不存在）或 value == ""。
		// 注意：Get 不会因网络失败返回给上层（clerk 会重试到有 reply），
		// ErrNoKey 不是错误，而是「锁空闲」的一种正常状态。
		// 错误写法 if err != rpc.OK || value != "" 会把 ErrNoKey 也 continue 掉，
		// 导致首次抢锁永远无法 Put，测试挂死。
		if err != rpc.ErrNoKey && value != "" {
			continue
		}

		err = lk.ck.Put(lk.l, lk.id, version)
		switch err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			if v, _, gerr := lk.ck.Get(lk.l); gerr == rpc.OK && v == lk.id {
				return
			}
			//认为锁暂时被其他客户端持有，或者没有成功持有锁
			//不写任何东西，等于continue继续循环
		case rpc.ErrVersion:
			//锁已经被其他客户端持有了，继续循环
			//不写任何东西，等于continue继续循环
		}
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.l)

		//理由同上，一个case
		if err != rpc.OK || val != lk.id {
			return
		}

		switch lk.ck.Put(lk.l, "", ver) {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			//锁整体被删除或者被其他客户端持有了
			if v, _, err := lk.ck.Get(lk.l); err != rpc.OK || v != lk.id {
				return
			}
			//否则认为锁仍被持有，继续循环尝试解锁
		case rpc.ErrVersion:
			//事实上不会被触发
			// Version changed; re-Get and retry if we still own it.
		}
	}
}
