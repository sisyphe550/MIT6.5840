package raft

import "time"

const activeWindowWidth = 2 * baseElectionTimeout * time.Millisecond

type PeerTracker struct {
	nextIndex  int
	matchIndex int

	lastAck time.Time
}

func (rf *Raft) quorumActive() bool {
	activePeers := 1
	for i, tracker := range rf.peerTrackers {
		if i != rf.me && time.Since(tracker.lastAck) <= activeWindowWidth {
			activePeers++
		}
	}
	return 2*activePeers >= len(rf.peers)
}
