package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const submitTimeout = 500 * time.Millisecond

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	Payload   any
}

type result struct {
	err   rpc.Err
	value any
}

type pendingCall struct {
	term      int
	requestId int64
	ch        chan result
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
	pending       map[int]*pendingCall
	lastApplied   int
	nextRequestId int64
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
		pending:      make(map[int]*pendingCall),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.run()

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
	if rsm.rf == nil {
		return rpc.ErrWrongLeader, nil
	}

	op := rsm.newOp(req)
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader || index == -1 {
		return rpc.ErrWrongLeader, nil
	}

	call := &pendingCall{
		term:      term,
		requestId: op.RequestId,
		ch:        make(chan result, 1),
	}

	rsm.mu.Lock()
	rsm.pending[index] = call
	rsm.mu.Unlock()

	timer := time.NewTimer(submitTimeout)
	defer timer.Stop()

	for {
		select {
		case res, ok := <-call.ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			return res.err, res.value
		case <-timer.C:
			curTerm, stillLeader := rsm.rf.GetState()
			if !stillLeader || curTerm != term {
				rsm.removePending(index, call)
				return rpc.ErrWrongLeader, nil
			}
			timer.Reset(submitTimeout)
		}
	}

	// return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

func (rsm *RSM) run() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.handleCommand(msg)
			continue
		}
		if msg.SnapshotValid {
			continue
		}
	}
	rsm.failAllPending(rpc.ErrWrongLeader)
}

func (rsm *RSM) handleCommand(msg raftapi.ApplyMsg) {
	op, ok := msg.Command.(Op)
	if !ok {
		return
	}

	if !rsm.markApplied(msg.CommandIndex) {
		return
	}

	reply := rsm.sm.DoOp(op.Payload)

	rsm.mu.Lock()
	call := rsm.pending[msg.CommandIndex]
	if call != nil {
		delete(rsm.pending, msg.CommandIndex)
	}
	rsm.mu.Unlock()

	if call != nil {
		res := result{value: reply}
		if call.requestId == op.RequestId {
			res.err = rpc.OK
		} else {
			res.err = rpc.ErrWrongLeader
		}
		call.ch <- res
	}
}

func (rsm *RSM) markApplied(index int) bool {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if index <= rsm.lastApplied {
		return false
	}
	rsm.lastApplied = index
	return true
}

func (rsm *RSM) removePending(index int, call *pendingCall) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if current, ok := rsm.pending[index]; ok && current == call {
		delete(rsm.pending, index)
		close(call.ch)
	}
}

func (rsm *RSM) failAllPending(err rpc.Err) {
	rsm.mu.Lock()
	pending := make([]*pendingCall, 0, len(rsm.pending))
	for idx, call := range rsm.pending {
		delete(rsm.pending, idx)
		pending = append(pending, call)
	}
	rsm.mu.Unlock()

	for _, call := range pending {
		call.ch <- result{err: err}
	}
}

func (rsm *RSM) newOp(req any) Op {
	requestId := atomic.AddInt64(&rsm.nextRequestId, 1)
	return Op{
		ClientId:  int64(rsm.me),
		RequestId: requestId,
		Payload:   req,
	}
}
