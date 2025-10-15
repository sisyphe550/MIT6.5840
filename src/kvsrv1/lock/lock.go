package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key   string
	owner string // unique token identifying this lock owner
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l}
	// generate unique owner token per lock instance
	lk.owner = fmt.Sprintf("owner-%d-%p", time.Now().UnixNano(), lk)
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			switch lk.ck.Put(lk.key, lk.owner, 0) {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				// verify the final state; if it's 1, we own the lock
				if v2, _, e2 := lk.ck.Get(lk.key); e2 == rpc.OK && v2 == lk.owner {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err == rpc.OK && val == "" {
			switch lk.ck.Put(lk.key, lk.owner, ver) {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				if v2, _, e2 := lk.ck.Get(lk.key); e2 == rpc.OK && v2 == lk.owner {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// already locked by someone
		if err == rpc.OK && val == lk.owner {
			// we already own it (idempotent Acquire)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			switch lk.ck.Put(lk.key, "", 0) {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				if v2, _, e2 := lk.ck.Get(lk.key); e2 == rpc.OK && v2 == "" {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err == rpc.OK && val == lk.owner {
			switch lk.ck.Put(lk.key, "", ver) {
			case rpc.OK:
				return
			case rpc.ErrMaybe:
				if v2, _, e2 := lk.ck.Get(lk.key); e2 == rpc.OK && v2 == "" {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err == rpc.OK && val == "" {
			return
		}
		// locked by someone else; wait
		time.Sleep(10 * time.Millisecond)
	}
}
