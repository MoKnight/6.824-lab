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
	//	"bytes"

	//"fmt"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A part
	state       int //当前状态
	currentTerm int //当前任期
	votedFor    int //votefor?

	electionTimer     *time.Timer
	heartbeatTimer    *time.Timer
	heartbeatInterval time.Duration
	electionInterval  time.Duration
	rpcTimeout        time.Duration

	//2B part
	log []Log
	// logqueue chan *Log

	commitIndex int
	//commitTerm  int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	//applyqueue chan ApplyMsg
	applyCh chan ApplyMsg
}

//all the state uesd for state
const (
	StateType = iota
	leader    //leader type
	follower  //follower type
	candidate //candidiate type
)

type Log struct {
	Command interface{} //command to be executed
	Term    int         //identical term
	Index   int         //identical index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	//2C part
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votefor, term, commitIndex int
	var log []Log
	if d.Decode(&votefor) != nil ||
		d.Decode(&term) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&commitIndex) != nil {
		DPrintf("fail to read persist data")
		return
	} else {
		rf.votedFor = votefor
		rf.currentTerm = term
		rf.log = log
		rf.commitIndex = commitIndex
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A part
	Term        int //candidate’s term
	CandidateId int //candidate requesting vote
	//2B part
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//2A part
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//2A part
type AppendEntriesReply struct {
	//2A part
	Term    int  //leader’s term
	Success bool //whether it's succeed
	//2B part
	LogIndex int
	LogTerm  int
}

type AppendEntriesArgs struct {
	Term         int   //leader’s term
	LeaderId     int   //so follower can redirect clients
	PrevLogIndex int   //index of log entry immediately precedingnew ones
	PrevLogTerm  int   //term of prevLogIndex entry
	Entries      []Log //log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
	CommitTerm   int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	RPCTimeout := rf.rpcTimeout

	t := time.NewTimer(RPCTimeout) //send timeout
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout) //send repeatly
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := AppendEntriesReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, &r)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.Success = r.Success
				reply.LogIndex = r.LogIndex
				reply.LogTerm = r.LogTerm
				return
			}
		}
	}
}

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = args.Term
	reply.VoteGranted = false

	if rf.currentTerm < args.Term { //remember to set Votefor

		//change the order
		rf.convertTo(follower)
		rf.votedFor = -1
		rf.currentTerm = args.Term

		rf.persist()

		// Election restriction
		_, commitlogterm := rf.searchLogByIndex(rf.commitIndex)
		if args.LastLogTerm < commitlogterm {
			return
		} else if args.LastLogTerm == commitlogterm {
			if args.LastLogIndex < rf.commitIndex {
				return
			}
		}

		rf.votedFor = args.CandidateId
		// rf.currentTerm = args.Term
		reply.VoteGranted = true
		if rf.state == follower {
			rf.electionTimer.Reset(rf.electionInterval)
		}

		//DPrintf("%d send vote for %d in term %d my index %d", rf.me, args.CandidateId, args.Term, rf.commitIndex)
	} else if args.Term == rf.currentTerm {
		if rf.state == leader {
			return
		}

		if rf.votedFor == args.CandidateId {
			rf.electionTimer.Reset(rf.electionInterval)
			reply.VoteGranted = true
			return
		}

		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			// vote for other server
			return
		}

		// no wote
	} else {
		reply.Term = rf.currentTerm
		//DPrintf("%d don't vote for %d in term %d", rf.me, args.CandidateId, args.Term)
		return
	}

	rf.persist()
	// rf.electionTimer.Stop()
	// rf.electionTimer.Reset(rf.electionInterval)
}

//2B part
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LogIndex = rf.commitIndex
	_, reply.LogTerm = rf.searchLogByIndex(rf.commitIndex)

	//synchronize term part
	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm == args.Term {
		rf.votedFor = -1
		rf.persist()

		if rf.state == follower { //normal state
			rf.electionTimer.Reset(rf.electionInterval)

		} else if rf.state == candidate { //stop my candidate phase
			rf.convertTo(follower)
			rf.persist()
		} else { // leader return directly
			return
		}
		reply.Success = true

	} else if rf.currentTerm < args.Term {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == follower {
			rf.electionTimer.Reset(rf.electionInterval)
		}
		rf.convertTo(follower)
		reply.Success = true

		rf.persist()
	}

	//replicate log part
	if len(args.Entries) != 0 {
		if index, exist := rf.searchLog(args.PrevLogIndex, args.PrevLogTerm); exist {
			for _, l := range args.Entries {
				// DPrintf("server %d append log %d in term %d", rf.me, l.Index, l.Term)
				rf.appendLog(index+1, l)
				index = len(rf.log) - 1
			}
			_, reply.LogIndex = rf.getLastTermIndex() //refresh

		} else {
			if rf.log[len(rf.log)-1].Term >= args.PrevLogTerm {
				reply.LogTerm, reply.LogIndex = rf.lastSmallTermIndex(args.PrevLogTerm)
			} else {
				reply.LogTerm, reply.LogIndex = rf.lastSmallTermIndex(rf.log[len(rf.log)-1].Term)
			}
			reply.Success = false

			// DPrintf("can't add log in term %d idx %d", args.Entries[0].Term, args.Entries[0].Index)
			// DPrintf(" prelogindex %d prelogterm %d", args.PrevLogIndex, args.PrevLogTerm)
			// DPrintf("next try log in term %d idx %d", reply.LogTerm, reply.LogIndex)
			return //just return,do nothing in commit part
		}
	}

	//commit log part
	if args.LeaderCommit > rf.commitIndex {
		// DPrintf("can commit now")
		rf.commit(args.LeaderCommit, args.CommitTerm)
	}
}

//2B part
func (rf *Raft) appendLog(index int, log Log) {
	if len(rf.log) == 0 {
		rf.log = append(rf.log, log)
	} else {
		rf.log = rf.log[:index]
		rf.log = append(rf.log, log)
	}

	rf.persist()
}

func (rf *Raft) commit(logindex int, logterm int) {
	if len(rf.log) > 0 && rf.log[len(rf.log)-1].Index >= logindex {

		if _, exist := rf.searchLog(logindex, logterm); !exist {
			return
		}

		//DPrintf("i'm sever%d,commit on log %d", rf.me, logindex)
		eindex, _ := rf.searchLogByIndex(logindex)
		sindex, _ := rf.searchLogByIndex(rf.commitIndex)
		rf.commitIndex = logindex

		rf.persist()
		// if
		// DPrintf("start from %d to %d", sindex, eindex)

		for _, l := range rf.log[sindex+1 : eindex+1] {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: l.Index,
			}
		}
		//rf.lastApplied = rf.commitIndex
	}

}

func (rf *Raft) searchLog(logindex int, logterm int) (realidx int, exist bool) {
	if len(rf.log) == 0 || (logindex == 0 && logterm == 0) {
		return 0, true
	}

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == logindex && rf.log[i].Term == logterm {
			return i, true
		}
	}

	return -1, false
}

//search the realidx,term by a log's index
func (rf *Raft) searchLogByIndex(logindex int) (realidx int, term int) {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == logindex {
			return i, rf.log[i].Term
		}
	}

	return -1, 0

}

func (rf *Raft) lastSmallTermIndex(expectterm int) (term int, index int) {
	term, index = 0, 0
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term > expectterm {
			continue
		}

		if rf.log[i].Term <= expectterm { //first one log having term smaller than expectterm
			term, index = rf.log[i].Index, rf.log[i].Term
			for j := i; j >= 0; j-- {
				if rf.log[i].Term == term {
					index = rf.log[i].Index
				} else {
					return term, index //return the first log index in fisrt term that smaller than expectterm
				}
			}

		}
	}

	return term, index
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//rpc timer
	RPCTimeout := rf.rpcTimeout

	t := time.NewTimer(RPCTimeout) //send timeout
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout / 2) //send repeatly
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			}
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) == 0 {
		index = 1
	} else {
		index = rf.log[len(rf.log)-1].Index + 1
	}

	term = rf.currentTerm

	if rf.state != leader {
		isLeader = false
	} else {
		log := Log{Command: command, Index: index, Term: rf.currentTerm}
		rf.appendLog(len(rf.log), log)
		rf.matchIndex[rf.me] = log.Index
		//rf.sendAppendEntriesPeriodically() //loop func ned to use goroutine
		//rf.heartbeatTimer.Reset(rf.heartbeatInterval)
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

// i think it's main loop
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//2A part

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			//DPrintf("%d timeout", rf.me)
			if rf.state == follower {
				// rf.startElection() is called in conversion to Candidate
				rf.convertTo(candidate) //enter this func need to get lock first
			} else {
				rf.start_election() //enter this func need to get lock first
			}
			rf.electionTimer.Reset(rf.electionInterval)
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == leader {
				// rf.sendHeartbeatPeriodically()
				rf.sendAppendEntriesPeriodically()
				rf.heartbeatTimer.Reset(rf.heartbeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}

// should be called with a lock
func (rf *Raft) convertTo(s int) {
	if s == rf.state {
		return
	}
	// rf.cond.Broadcast()
	// DPrintf("Term %d: server %d convert from %v to %v\n",
	// 	rf.currentTerm, rf.me, rf.state, s)
	rf.state = s
	switch s {
	case follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(rf.electionInterval)
		rf.votedFor = -1

	case candidate:
		rf.start_election()

	case leader:
		_, lastLogIndex := rf.getLastTermIndex()

		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = lastLogIndex

		rf.electionTimer.Stop()
		rf.sendHeartbeatPeriodically()
		rf.heartbeatTimer.Reset(rf.heartbeatInterval)
	}
}

func (rf *Raft) sendHeartbeatPeriodically() {
	// DPrintf("%d send heartbeat in term %d", rf.me, rf.currentTerm)
	if rf.state != leader {
		return
	}
	ResCh := make(chan bool, len(rf.peers))

	for num, _ := range rf.peers {

		if rf.killed() || rf.state != leader {
			return
		}

		if num != rf.me {
			go func(ch chan bool, num int) {

				rf.mu.Lock()
				request := rf.makeAppendEntriesArgs(num)
				request.Entries = []Log{}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()

				rf.sendAppendEntries(num, &request, &reply)
				// DPrintf("i'm leader %d,opps, server %d seems to be down", rf.me, num)

				ch <- reply.Success
				rf.mu.Lock()
				if reply.Success {
					rf.nextIndex[num] = reply.LogIndex + 1
					rf.matchIndex[num] = reply.LogIndex
				} else if !reply.Success && reply.Term > rf.currentTerm {
					//DPrintf("sorry server%d have to change", rf.me)
					rf.currentTerm = reply.Term
					rf.convertTo(follower)
					rf.persist()
				}
				rf.mu.Unlock()

			}(ResCh, num)
		}
	}

}

func (rf *Raft) sendAppendEntriesPeriodically() {
	// DPrintf("%d send AppendEntries in term %d", rf.me, rf.currentTerm)
	// ResCh := make(chan bool, len(rf.peers)+10)

	for num, _ := range rf.peers {
		if rf.killed() || rf.state != leader {
			return
		}

		if num != rf.me {
			go rf.sendAppendEntriesToPeer(num)
		}
	}
	// rf.electionTimer.Reset(time.Duration(rand.Int63n(500)+1000) * time.Millisecond)
}

func (rf *Raft) sendAppendEntriesToPeer(num int) {
	rf.mu.Lock()
	request := rf.makeAppendEntriesArgs(num)
	// if len(request.Entries) != 0 {
	// 	DPrintf("length :%d from %d to %d", len(request.Entries), request.Entries[0].Index, request.Entries[len(request.Entries)-1].Index)
	// }

	reply := AppendEntriesReply{}
	rf.sendAppendEntries(num, &request, &reply)

	// ch <- reply.Success

	if rf.state == leader && !rf.killed() {
		if reply.Success { //return success

			rf.matchIndex[num] = reply.LogIndex
			rf.nextIndex[num] = reply.LogIndex + 1

			for i := len(rf.log) - 1; i >= 0 && rf.log[i].Index > rf.commitIndex; i-- { //check every log in leader'log to see whether it's ready to commit
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= rf.log[i].Index {
						count += 1
					}
				}

				if count > len(rf.peers)/2 && rf.state == leader {
					// most of nodes agreed on rf.logs[i]
					DPrintf("i'm leader %d commit now in log %d in term %d", rf.me, rf.log[i].Index, rf.currentTerm)
					rf.commit(rf.log[i].Index, rf.log[i].Term)
					break
				}
			}

		} else {
			if reply.Term > rf.currentTerm {
				DPrintf("i'm last leader%d in term %d by %d in term %d", rf.me, rf.currentTerm, num, reply.Term)
				rf.currentTerm = reply.Term
				rf.convertTo(follower)

				rf.persist()

			} else if reply.LogIndex != 0 {
				//add the lost logs
				index, exist := rf.searchLog(reply.LogIndex, reply.LogTerm)
				if !exist {
					//send again
					//find the last small term before the reply.LogTerm
					_, logindex := rf.lastSmallTermIndex(reply.LogTerm)
					index, _ = rf.searchLogByIndex(logindex)
					if index == -1 {
						index = 0
					}
					rf.nextIndex[num] = rf.log[index].Index
				} else {
					DPrintf("i'm leader %d set the nextIndex at %d for server %d", rf.me, rf.log[index].Index+1, num)
					rf.nextIndex[num] = rf.log[index].Index + 1
				}
				// time.Sleep(10 * time.Millisecond)
				// rf.mu.Unlock()
				DPrintf("send again")
				//rf.sendAppendEntriesToPeer(num) // in false state, we should send again
				// return
			} else if reply.LogIndex == 0 && len(request.Entries) != 0 { //start from every begining
				rf.nextIndex[num] = rf.log[0].Index
				// time.Sleep(10 * time.Millisecond)
				// rf.mu.Unlock()
				// DPrintf("send again")
				// rf.sendAppendEntriesToPeer(num) // in false state, we should send again
				// return
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) makeRequestVoteArgs() RequestVoteArgs {
	_, lastlogterm := rf.searchLogByIndex(rf.commitIndex)
	// lastlogterm, lastlastlogindex := rf.getLastTermIndex()
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  lastlogterm,
	}
	return request
}

func (rf *Raft) makeAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	prevlogindex, prevlogterm, logs := rf.getprelogtermindex(peerIdx)
	_, commitTerm := rf.searchLogByIndex(rf.commitIndex)
	request := AppendEntriesArgs{
		Term:         rf.currentTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
		CommitTerm:   commitTerm,
		PrevLogIndex: prevlogindex,
		PrevLogTerm:  prevlogterm,
		LeaderId:     rf.me,
	}
	return request
}

func (rf *Raft) getprelogtermindex(peerIdx int) (prevLogIndex, prevLogTerm int, res []Log) {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.getLastTermIndex()
	if nextIdx > lastLogIndex {
		//no log need to be sent
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	prevLogIndex = nextIdx - 1
	_, prevLogTerm = rf.searchLogByIndex(prevLogIndex)
	index, _ := rf.searchLogByIndex(nextIdx)
	res = append(res, rf.log[index:]...)
	return
}

func (rf *Raft) getLastTermIndex() (term int, index int) {
	if len(rf.log) == 0 {
		return 0, 0
	} else {
		return rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Index
	}

}

//send the RequestVote in parallel, not count the votes

func (rf *Raft) start_election() {
	rf.currentTerm += 1
	requestvote := 1
	rf.votedFor = rf.me
	rf.persist()

	for num, _ := range rf.peers {
		//turn off the server at once
		if rf.killed() || rf.state != candidate {
			return
		}

		if num != rf.me {
			go func(num int) {

				rf.mu.Lock()
				request := rf.makeRequestVoteArgs()
				reply := RequestVoteReply{}
				rf.mu.Unlock()

				rf.sendRequestVote(num, &request, &reply)

				rf.mu.Lock()
				if rf.state == candidate && !rf.killed() {
					if reply.VoteGranted {
						requestvote++
						if requestvote > len(rf.peers)/2 {
							rf.convertTo(leader)
							DPrintf("i'm leader %d my index is %d in term %d", rf.me, rf.commitIndex, rf.currentTerm)
						}
					} else {
						if reply.Term > rf.currentTerm {
							DPrintf("hear from %d, i'm server %d, can't be leader my comiitindex  %d in term %d from term %d", num, rf.me, rf.commitIndex, rf.currentTerm, reply.Term)
							rf.currentTerm = reply.Term
							rf.convertTo(follower)
							rf.persist()
						} else {
							// DPrintf("i'm server %dwhat happened", rf.me)
						}

					}
				}

				rf.mu.Unlock()

			}(num)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	heartbeatinterval := time.Duration(rand.Int63n(20)+100) * time.Millisecond //timeout wait for heartbeat
	electioninterval := time.Duration(rand.Int63n(200)+400) * time.Millisecond
	rf := &Raft{
		currentTerm:       0,
		state:             follower,
		heartbeatInterval: heartbeatinterval,
		electionInterval:  electioninterval,
		votedFor:          -1,
		lastApplied:       0,
		commitIndex:       0,
		me:                me,
		peers:             peers,
		persister:         persister,
		applyCh:           applyCh,
	}

	rf.electionTimer = time.NewTimer(rf.electionInterval)
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatInterval)
	rf.rpcTimeout = time.Duration(100) * time.Millisecond
	rf.heartbeatTimer.Stop()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	//DPrintf("%d initialize", rf.me)
	go rf.ticker()

	return rf
}
