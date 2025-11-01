package raft

import "6.5840/raftapi"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}

	if rf.log.FirstLogIndex <= index {
		if index > rf.lastApplied {
			panic("Snapshot index is greater than last applied index")
		}
		rf.snopshot = snapshot
		rf.snopshotLastIncludeIndex = index
		rf.snopshotLastIncludeTerm = rf.getEntryTerm(index)

		newFirstLogIndex := index + 1
		if newFirstLogIndex <= rf.log.LastLogIndex {
			rf.log.Entries = rf.log.Entries[newFirstLogIndex-rf.log.FirstLogIndex:]
		} else {
			rf.log.LastLogIndex = newFirstLogIndex - 1
			rf.log.Entries = make([]Entry, 0)
		}
		rf.log.FirstLogIndex = newFirstLogIndex
		rf.commitIndex = max(rf.commitIndex, index)
		rf.lastApplied = max(rf.lastApplied, index)

		rf.persist()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.InstallSnapshot(i)
		}
	}
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.state = follower
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
	defer rf.persist()

	if args.LastIncludeIndex > rf.snopshotLastIncludeIndex {
		rf.snopshot = args.Snapshot
		rf.snopshotLastIncludeIndex = args.LastIncludeIndex
		rf.snopshotLastIncludeTerm = args.LastIncludeTerm
		if args.LastIncludeIndex >= rf.log.LastLogIndex {
			rf.log.Entries = make([]Entry, 0)
			rf.log.LastLogIndex = args.LastIncludeIndex
		} else {
			rf.log.Entries = rf.log.Entries[rf.log.getRealIndex(args.LastIncludeIndex+1):]
		}
		rf.log.FirstLogIndex = args.LastIncludeIndex + 1

		if args.LastIncludeIndex > rf.lastApplied {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Snapshot,
				SnapshotIndex: args.LastIncludeIndex,
				SnapshotTerm:  args.LastIncludeTerm,
			}
			rf.applyHelper.tryApply(&msg)
			rf.lastApplied = args.LastIncludeIndex
		}
		rf.commitIndex = max(rf.commitIndex, args.LastIncludeIndex)
	}
}

func (rf *Raft) InstallSnapshot(serverId int) {
	rf.mu.Lock()
	if rf.state != leader {
		return
	}

	args := RequestInstallSnapshotArgs{}
	reply := RequestInstallSnapshotReply{}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludeIndex = rf.snopshotLastIncludeIndex
	args.LastIncludeTerm = rf.snopshotLastIncludeTerm
	args.Snapshot = rf.snopshot
	rf.mu.Unlock()

	ok := rf.sendRequestInstallSnapshot(serverId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.votedFor = None
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	rf.peerTrackers[serverId].nextIndex = args.LastIncludeIndex + 1
	rf.peerTrackers[serverId].matchIndex = args.LastIncludeIndex

	rf.tryCommitL(rf.peerTrackers[serverId].matchIndex)
}

func (rf *Raft) sendRequestInstallSnapshot(server int, args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	return ok
}
