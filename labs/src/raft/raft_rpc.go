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
}

// logic: if the candidate's term is less than the receiver's term, the receiver rejects the vote
// if the candidate's term is greater than the receiver's term, the receiver votes for the candidate
// if the candidate's term is equal to the receiver's term, the receiver votes for the candidate if it has not voted for anyone else
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "Server %v received vote request from %v with term %v", rf.me, args.CandidateId, args.Term)

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
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dAppend, "Server %v received appendEntries from %v with term %v", rf.me, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	// logMatch := true // TODO: check if log matches

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.demoteToFollower(args.Term)
	}

	// TODO: process log entries

	rf.resetElectionTimer()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
			ok := rf.sendRequestVote(p, args, reply)
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

			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.log) - 1,
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.demoteToFollower(reply.Term)
			}
			rf.mu.Unlock()
		}(i)
	}
}
