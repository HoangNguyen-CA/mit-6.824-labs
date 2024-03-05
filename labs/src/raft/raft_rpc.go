package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for fast rollback
	XIndex int
	XLen   int
}

// logic: if the candidate's term is less than the receiver's term, the receiver rejects the vote
// if the candidate's term is greater than the receiver's term, the receiver votes for the candidate
// if the candidate's term is equal to the receiver's term, the receiver votes for the candidate if it has not voted for anyone else
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "Server %v received requestVote | term: %v, candidateId: %v, lastLogIndex: %v, lastLogTerm: %v", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.demoteToFollower(args.Term)
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	logUpdated := true

	if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		logUpdated = false
	}

	if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
		logUpdated = false
	}

	if !logUpdated {
		return
	}

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if canVote {
		Debug(dVote, "Server %v voted for %v", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dAppend, "Server %v received appendEntries | term: %v, leaderId: %v, prevLogIndex: %v, prevLogTerm: %v, leaderCommit: %v, entries: %v", rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XIndex = -1
	reply.XLen = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.demoteToFollower(args.Term)
	} else {
		rf.resetElectionTimer()
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// send additional information for faster rollback
	if args.PrevLogIndex >= len(rf.log) {
		reply.XLen = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != term || i == 0 {
				reply.XIndex = i + 1
				break
			}
		}
		return
	}

	//3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	for _, entry := range args.Entries {
		if entry.Index >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else if rf.log[entry.Index].Term != entry.Term {
			rf.log = append(rf.log[:entry.Index], entry)
		}
	}

	rf.persist()

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) broadcastRequestVotes() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(p int) {

			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}

			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			ok := rf.peers[p].Call("Raft.RequestVote", args, reply)

			if !ok {
				return
			}

			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.demoteToFollower(reply.Term)
			}

			if reply.VoteGranted {
				rf.votes++
				if rf.votes > len(rf.peers)/2 {
					rf.mu.Unlock()
					rf.promoteToLeader()
					return
				}
			}

			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()

			Debug(dAppend, "Server %v sending appendEntries to %v | nextIndex: %v, logLength: %v", rf.me, i, rf.nextIndex[i], len(rf.log))

			//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex

			reqNextIndex := rf.nextIndex[i]
			var entries []LogEntry
			if rf.nextIndex[i] < len(rf.log) {
				entries = rf.log[rf.nextIndex[i]:]
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				Entries:      entries,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.demoteToFollower(reply.Term)
			}

			//If successful: update nextIndex and matchIndex for follower (§5.3)
			//If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)

			// if reply.Success {
			// 	rf.nextIndex[i] = reqNextIndex + len(entries)
			// 	rf.matchIndex[i] = rf.nextIndex[i] - 1
			// } else {
			// 	rf.nextIndex[i]--
			// }

			updatedNextIndex := reqNextIndex + len(entries)
			if reply.Success && updatedNextIndex > rf.nextIndex[i] {
				rf.nextIndex[i] = updatedNextIndex
				rf.matchIndex[i] = rf.nextIndex[i] - 1
			} else if !reply.Success && (reply.XIndex >= 1 || reply.XLen >= 1) {
				Debug(dAppend, "Server %v appendEntries to %v failed | xIndex: %v, xLen: %v", rf.me, i, reply.XIndex, reply.XLen)
				if reply.XLen >= 0 {
					rf.nextIndex[i] = reply.XLen
				} else {
					rf.nextIndex[i] = reply.XIndex
				}
			}

			rf.mu.Unlock()
		}(i)
	}
}
