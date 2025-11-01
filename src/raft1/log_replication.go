package raft

import "time"

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

// // 心跳兼日志同步处理器
// func (rf *Raft) HandleAppendEntriesRPC2(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	reply.FollowerTerm = rf.currentTerm
// 	reply.Success = true
// 	// 旧时期的 leader 抛弃掉
// 	if args.LeaderTerm < rf.currentTerm {
// 		reply.Success = false
// 		return
// 	}
// 	rf.resetElectionTimer()
// 	rf.state = follower

// 	if args.LeaderTerm > rf.currentTerm {
// 		rf.votedFor = None
// 		rf.currentTerm = args.LeaderTerm
// 		reply.FollowerTerm = rf.currentTerm
// 	}

// 	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex || rf.log.getEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
// 		reply.FollowerTerm = rf.currentTerm
// 		reply.Success = false
// 		reply.PrevLogIndex = rf.log.LastLogIndex
// 		reply.PrevLogTerm = rf.getLastEntryTerm()
// 	} else if rf.log.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm {
// 		ok := true
// 		for i, entry := range args.Entries {
// 			index := args.PrevLogIndex + 1 + i
// 			if index > rf.log.LastLogIndex {
// 				rf.log.appendL(entry)
// 			} else if rf.log.getOneEntry(index).Term != entry.Term {
// 				// 覆写
// 				ok = false
// 				*rf.log.getOneEntry(index) = entry
// 			}
// 		}
// 		if !ok {
// 			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
// 		}
// 		if args.LeaderCommit > rf.commitIndex {
// 			if args.LeaderCommit < rf.log.LastLogIndex {
// 				rf.commitIndex = args.LeaderCommit
// 			} else {
// 				rf.commitIndex = rf.log.LastLogIndex
// 			}
// 			rf.applyCond.Broadcast()
// 		}
// 		reply.FollowerTerm = rf.currentTerm
// 		reply.Success = true
// 		reply.PrevLogIndex = rf.log.LastLogIndex
// 		reply.PrevLogTerm = rf.getLastEntryTerm()
// 	}
// }

// nextIndex收敛速度优化：nextIndex跳跃算法
// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) HandleAppendEntriesRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock() // 加接收日志方的锁
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的leader抛弃掉
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.resetElectionTimer()
	rf.state = follower // 需要转变自己的保持为Follower

	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None // 调整votedFor为-1
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}

	defer rf.persist()

	if rf.log.empty() {
		if args.PrevLogIndex == rf.snopshotLastIncludeIndex {
			rf.log.appendL(args.Entries...)
			reply.FollowerTerm = rf.currentTerm
			reply.Success = true
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
			return
		} else {
			reply.FollowerTerm = rf.currentTerm
			reply.Success = false
			reply.PrevLogIndex = rf.log.LastLogIndex
			reply.PrevLogTerm = rf.getLastEntryTerm()
		}
	}

	if args.PrevLogIndex+1 < rf.log.FirstLogIndex || args.PrevLogIndex > rf.log.LastLogIndex {
		// DPrintf(111, "args.PrevLogIndex is %d, out of index...", args.PrevLogIndex)
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
	} else if rf.getEntryTerm(args.PrevLogIndex) == args.PrevLogTerm {
		ok := true
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.log.LastLogIndex {
				rf.log.appendL(entry)
			} else if rf.log.getOneEntry(index).Term != entry.Term {
				// 采用覆盖写的方式
				ok = false
				*rf.log.getOneEntry(index) = entry
			}
		}
		if !ok {
			rf.log.LastLogIndex = args.PrevLogIndex + len(args.Entries)
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.log.LastLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log.LastLogIndex
			}
			rf.applyCond.Broadcast()
		}
		reply.FollowerTerm = rf.currentTerm
		reply.Success = true
		reply.PrevLogIndex = rf.log.LastLogIndex
		reply.PrevLogTerm = rf.getLastEntryTerm()
		// DPrintf(200, "%v:log entries was overrited, added or done nothing, updating commitIndex to %d...", rf.SayMeL(), rf.commitIndex)
	} else {
		prevIndex := args.PrevLogIndex
		for prevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(prevIndex) == rf.log.getOneEntry(args.PrevLogIndex).Term {
			prevIndex--
		}
		//prevIndex++ // 当前任期提交的第一个日志
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		if prevIndex >= rf.log.FirstLogIndex {
			reply.PrevLogIndex = prevIndex
			reply.PrevLogTerm = rf.getEntryTerm(prevIndex)
			// DPrintf(111, "%v: stepping over the index of currentTerm to the last log entry of last term", rf.SayMeL())
		} else {
			reply.PrevLogIndex = rf.snopshotLastIncludeIndex
			reply.PrevLogTerm = rf.snopshotLastIncludeTerm
		}
	}

}

// 主节点对日志进行提交，条件是多数从节点的commitIndex >= matchIndex(leader)
func (rf *Raft) tryCommitL(matchIndex int) {
	// matchIndex 必须大于 commitIndex 的才能提交，因为小于等于 commitIndex 的日志已经提交
	if matchIndex < rf.commitIndex {
		return
	}
	// 越界的也不能提交
	if matchIndex > rf.log.LastLogIndex {
		return
	}
	if matchIndex < rf.log.FirstLogIndex {
		return
	}

	// 提交的日志必须是当前任期内收到的日志
	if rf.getEntryTerm(matchIndex) != rf.currentTerm {
		return
	}

	// 计算所有已经正确匹配该 matchindex 的从节点票数
	cnt := 1 // leader 自己投一票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if matchIndex <= rf.peerTrackers[i].matchIndex {
			cnt++
		}
	}

	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex
		if rf.commitIndex > rf.log.LastLogIndex {
			panic("commitIndex > LastLogIndex")
		}
		rf.applyCond.Broadcast()
	}
}
