package raft

import (
	"math/rand"
	"time"
)

const baseElectionTimeout = 300
const None = -1

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		return
	}

	// rf.resetElectionTimer()
	rf.becomeCandidate()

	term := rf.currentTerm
	done := false
	votes := 1
	args := RequestVoteArgs{}
	args.Term = term
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log.LastLogIndex
	args.LastLogTerm = rf.getLastEntryTerm()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)

			if !ok || !reply.VoteGranted {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm > reply.Term {
				return
			}

			votes++
			if done || votes <= len(rf.peers)/2 {
				return
			}
			rf.becomeLeader()
			go rf.StartAppendEntries(true)
		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = follower
	rf.votedFor = None
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.resetTrackedIndex()
}

func (rf *Raft) HandleHeartbeatRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.FollowerTerm = rf.currentTerm
	reply.Success = true

	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.resetElectionTimer()
	rf.state = follower
	rf.votedFor = args.LeaderId

	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = None
		rf.state = follower
	}

	update := false
	update = update || args.LastLogTerm > rf.getLastEntryTerm()
	update = update || args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.log.LastLogIndex

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		rf.votedFor = args.CandidateId
		rf.state = follower
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}
}
