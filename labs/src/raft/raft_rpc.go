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

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		Debug(dVote, "Server %v rejected vote request from %v with lower term %v", rf.me, args.CandidateId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
		rf.votedFor = -1
	}

	logUpdated := true // TODO: check if log is up to date
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if canVote && logUpdated {
		Debug(dVote, "Server %v voted for %v", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	} else {
		Debug(dVote, "Server %v with votedFor = %v rejected vote request from %v", rf.me, rf.votedFor, args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dAppend, "Server %v received appendEntries from %v with term %v", rf.me, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	reply.Success = false

	logMatch := true // TODO: check if log matches

	if args.Term < rf.currentTerm || !logMatch {
		return
	}

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.currentTerm = args.Term
		rf.convertToFollower()
		rf.votedFor = -1
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

// called conccurently in a goroutine
func (rf *Raft) broadcastAppendEntries() {
	// for i := range rf.peers {
	// 	if i == rf.me {
	// 		continue
	// 	}

	// 	go func(i int) {
	// 		args := &AppendEntriesArgs{
	// 			Term:     rf.currentTerm,
	// 			LeaderId: rf.me,
	// 		}
	// 		reply := &AppendEntriesReply{}
	// 		rf.sendAppendEntries(i, args, reply)
	// 	}(i)
	// }
}
