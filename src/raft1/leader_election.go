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

	rf.resetElectionTimer()
	rf.becomeCandidate()

	done := false
	votes := 1
	// term := rf.currentTerm
	args := RequestVoteArgs{rf.currentTerm, rf.me}

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
			rf.state = leader
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
}

func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = follower
	rf.votedFor = None
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = follower
	}

	reply.Term = rf.currentTerm

	update := true
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		rf.votedFor = args.CandidateId
		rf.state = follower
		rf.resetElectionTimer()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}
