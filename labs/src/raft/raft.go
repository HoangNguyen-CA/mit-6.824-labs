package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 350
	ElectionTimeoutMax = 500
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistant state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//extra state
	votes   int
	state   State
	applyCh chan ApplyMsg

	lastHeartbeat time.Time
	timeoutDelay  time.Duration
}

// returns a random election timeout between ElectionTimeoutMin and ElectionTimeoutMax
func randElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
}

// resets the election timer using randElectionTimeout(). should only be called when:
// (1) receiving a valid AppendEntries RPC.
// (2) receiving a valid RequestVote RPC (and voting for the candidate).
// (3) converting to candidate (and starting election).
// requires lock
func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
	rf.timeoutDelay = randElectionTimeout()
}

// demote to follower and reset votedFor state. should only be called when:
// (1) if discovered rpc term is greater than currentTerm.
// (2) if is candidate and received AppendEntries from new leader.
// requires lock
func (rf *Raft) demoteToFollower(nTerm int) {
	Debug(dInfo, "Server %v demoted to follower", rf.me)
	rf.state = Follower
	rf.currentTerm = nTerm
	rf.votedFor = -1
	rf.votes = 0
	rf.persist()
}

// promote candidate to leader and start sending heartbeats. should only be called when:
// (1) is candidate and won election.
func (rf *Raft) promoteToLeader() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	Debug(dInfo, "Server %v promoted to leader", rf.me)

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	rf.state = Leader
	rf.mu.Unlock()
	rf.broadcastAppendEntries()
}

// converts to candidate and starts election. should only be called when:
// (1) is follower and election timer expires.
// (2) is candidate and election timer expires.
// call in goroutine to prevent waiting for RPCs
func (rf *Raft) convertToCandidate() {

	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	Debug(dVote, "Server %v started election for term %v", rf.me, rf.currentTerm+1)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votes = 1
	rf.resetElectionTimer()
	rf.persist()
	rf.mu.Unlock()

	rf.broadcastRequestVotes()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	isLeader := rf.state == Leader
	return currentTerm, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var pCurrentTerm int
	var pVotedFor int
	var pLog []LogEntry
	if d.Decode(&pCurrentTerm) != nil ||
		d.Decode(&pVotedFor) != nil || d.Decode(&pLog) != nil {
		log.Fatal("Error reading persisted state")
	} else {
		rf.currentTerm = pCurrentTerm
		rf.votedFor = pVotedFor
		rf.log = pLog
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm

	isLeader := rf.state == Leader

	if isLeader {
		Debug(dLog, "Server %v received command %v | term: %v, index: %v", rf.me, command, term, index)
		rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
		rf.persist()
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C)

	//persistent state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Index: 0, Command: nil}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//volatile state
	rf.commitIndex = 0
	rf.lastApplied = 0

	//leader volatile state
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//extra state
	rf.votes = 0
	rf.applyCh = applyCh
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	rf.timeoutDelay = randElectionTimeout()

	rf.mu.Unlock()

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)

			rf.mu.Lock()
			if time.Since(rf.lastHeartbeat) > rf.timeoutDelay {
				if rf.state != Leader {
					go rf.convertToCandidate()
				}
				rf.resetElectionTimer()
			}
			rf.mu.Unlock()
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)

			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				entry := rf.log[rf.lastApplied]

				rf.mu.Unlock()
				applyMsg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index}
				rf.applyCh <- applyMsg
				continue
			}
			rf.mu.Unlock()
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)

			rf.mu.Lock()
			//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)

			if rf.state == Leader {
				//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
				for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
					count := 1
					for i := range rf.peers {
						if i == rf.me {
							continue
						}
						if rf.matchIndex[i] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N].Term == rf.currentTerm {
						rf.commitIndex = N
						break
					}
				}

			}
			rf.mu.Unlock()
		}
	}()

	go func() {
		for !rf.killed() {
			time.Sleep(HeartbeatInterval)
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcastAppendEntries()
			}
			rf.mu.Unlock()
		}
	}()

	return rf
}
