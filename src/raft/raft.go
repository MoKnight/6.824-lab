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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	// VoteForTerm int
	// requestvote int        //收集到的选票
	cond *sync.Cond //条件锁
	// finish int        //already send vote request count
	// leader      int        //my leader

	electionTimer     *time.Timer // 2A
	heartbeatTimer    *time.Timer
	heartbeatInterval time.Duration
	electionInterval  time.Duration

	rpcTimeout time.Duration

	//log        []
}

//all the state uesd for state
const (
	StateType = iota
	leader    //leader type
	follower  //follower type
	candidate //candidiate type
)

// const (
// 	noflag = iota
// 	checktimeflag
// 	voteflag
// 	//todo
// )

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
	//lastLogIndex //index of candidate’s last log entry (§5.4)
	//lastLogTerm //term of candidate’s last log entry (§5.4)
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
type EasyMes struct {
	Term   int //leader’s term
	Accept bool
	Id     int //laederid
}

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
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.convertTo(follower)

		DPrintf("%d send vote for %d in term %d", rf.me, args.CandidateId, args.Term)
	} else if args.Term == rf.currentTerm {
		if rf.state == leader {
			return
		}
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			// vote for other server
			return
		}
		// no wote
	} else {
		DPrintf("%d don't vote for %d in term %d", rf.me, args.CandidateId, args.Term)
		return
	}

	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.electionInterval)

}

func (rf *Raft) Heartbeat(args *EasyMes, reply *EasyMes) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("i'm server %d, i get heartbeat from %d", rf.me, args.Id)

	reply.Term = rf.currentTerm
	if rf.currentTerm == args.Term { //stop my candidate phase
		rf.votedFor = -1

		if rf.state == follower { //normal state
			reply.Accept = true
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(rf.electionInterval)

		} else if rf.state == candidate { //normal state
			reply.Accept = true
			rf.convertTo(follower)
		} else {
			reply.Accept = false
		}

	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Accept = false
	} else if rf.currentTerm < args.Term {
		// DPrintf("%d get a new leader %d", rf.me, args.Id)
		rf.currentTerm = args.Term
		reply.Accept = true
		rf.votedFor = -1
		if rf.state == follower {
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(rf.electionInterval)
			return
		}
		rf.convertTo(follower)

	}
}

func (rf *Raft) sendHeartbeat(server int, args *EasyMes, reply *EasyMes) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	return ok
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
	//rpc定时器
	RPCTimeout := rf.rpcTimeout

	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
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
			DPrintf("%d timeout", rf.me)
			if rf.state == follower {
				// rf.startElection() is called in conversion to Candidate
				rf.convertTo(candidate) //enter this func need to get lock first
			} else {
				rf.start_election() //enter this func need to get lock first
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == leader {
				rf.sendHeartbeatPeriodically()
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
	rf.cond.Broadcast()
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
		rf.electionTimer.Stop()
		rf.sendHeartbeatPeriodically()
		rf.heartbeatTimer.Reset(rf.heartbeatInterval)
	}
}

func (rf *Raft) sendHeartbeatPeriodically() {
	DPrintf("%d send heartbeat in term %d", rf.me, rf.currentTerm)
	if rf.state != leader {
		return
	}

	for num, _ := range rf.peers {
		term, id := rf.currentTerm, rf.me

		if rf.killed() || rf.state != leader {
			return
		}

		if num != rf.me {
			go func(num int) {
				request := EasyMes{Term: term, Id: id}
				reply := EasyMes{}

				if ok := rf.sendHeartbeat(num, &request, &reply); !ok {
					//DPrintf("i'm leader %d,opps, server %d seems to be down", rf.me, num)
				} else {
					if !reply.Accept && reply.Term > rf.currentTerm {
						DPrintf("sorry server%d have to change", rf.me)
						rf.currentTerm = reply.Term
						rf.convertTo(follower)
					}
				}
			}(num)
		}
	}

}

//send the RequestVote in parallel, not count the votes

func (rf *Raft) start_election() {
	rf.electionTimer.Stop()
	rf.currentTerm++
	requestvote := 1
	finish := 1
	rf.votedFor = rf.me
	DPrintf("%d start to elect in term %d", rf.me, rf.currentTerm)
	term, id := rf.currentTerm, rf.me
	votesCh := make(chan bool, len(rf.peers))

	for num, _ := range rf.peers {
		//turn off the server at once
		if rf.killed() || rf.state != candidate {
			return
		}

		if num != rf.me {
			go func(ch chan bool, num int) {
				request := RequestVoteArgs{Term: term, CandidateId: id}
				reply := RequestVoteReply{}
				rf.sendRequestVote(num, &request, &reply)

				ch <- reply.VoteGranted

				rf.mu.Lock()
				finish++
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.convertTo(follower)
				} else if reply.VoteGranted && reply.Term == rf.currentTerm {
					requestvote++
				}
				rf.cond.Broadcast()
				rf.mu.Unlock()

			}(votesCh, num)
		}
	}

	rf.electionTimer.Reset(time.Duration(rand.Int63n(500)+1000) * time.Millisecond)

	for requestvote <= (len(rf.peers)/2) && finish != len(rf.peers) && !rf.killed() && rf.state == candidate {
		rf.cond.Wait()
		DPrintf("%d get %d votes in term %d", rf.me, requestvote, rf.currentTerm)
	}

	if requestvote > (len(rf.peers)/2) && !rf.killed() && rf.state == candidate && term == rf.currentTerm {
		DPrintf("%d is leader now", rf.me)
		rf.votedFor = -1
		rf.convertTo(leader)
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
	electioninterval := time.Duration(rand.Int63n(200)+300) * time.Millisecond
	rf := &Raft{
		currentTerm:       0,
		state:             follower,
		heartbeatInterval: heartbeatinterval,
		electionInterval:  electioninterval,
		votedFor:          -1,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.cond = sync.NewCond(&rf.mu)
	rf.electionTimer = time.NewTimer(rf.electionInterval)
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatInterval)
	rf.rpcTimeout = time.Duration(100) * time.Millisecond
	rf.heartbeatTimer.Stop()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf("%d initialize", rf.me)
	go rf.ticker()

	return rf
}
