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
	state            int           //当前状态
	heartbeattime    time.Time     //自身的过期等待时间
	heartbeattimeout time.Duration //固定的heartbeat时间
	electtime        time.Time     //限时elect
	needelect        bool
	currentTerm      int //当前任期
	votedFor         int //votefor?
	VoteForTerm      int
	requestvote      int        //收集到的选票
	cond             *sync.Cond //条件锁
	finish           int        //already send vote request count
	//commandchannel   chan int   //in normal state,wait for leader to send the command
	leader int //my leader
	//wg               sync.WaitGroup //waitgroup

	//log        []
}

//all the state uesd for state
const (
	StateType  = iota
	leader     //leader type
	follower   //follower type
	candidiate //candidiate type
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
	Term int //leader’s term
	Id   int //laederid
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term && (args.Term > rf.VoteForTerm || rf.votedFor == -1) { //remember to set Votefor
		rf.votedFor = args.CandidateId
		rf.VoteForTerm = args.Term
		reply.VoteGranted = true
		reply.Term = args.Term
		//rf.commandchannel <- checktimeflag

		DPrintf("%d send vote for %d in term %d", rf.me, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		DPrintf("%d don't vote for %d in term %d", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) Heartbeat(args *EasyMes, reply *EasyMes) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm == args.Term && rf.leader == args.Id { //normal state
		rf.heartbeattime = time.Now().Add(rf.heartbeattimeout)
		//rf.commandchannel <- checktimeflag
		//DPrintf("%d is still my leader", args.Id)
		// } else if rf.currentTerm == args.Term && rf.votedFor == args.Id { //first get heartbeat(the win heartbeat)

		// 	rf.leader = args.Id
		// 	rf.votedFor = -1
		// 	rf.state = follower
		// 	rf.heartbeattime = time.Now().Add(rf.heartbeattimeout)
		// 	//DPrintf("%d is still my leader", args.Id)

		// 	//rf.commandchannel <- checktimeflag
	} else if rf.currentTerm < args.Term {
		rf.heartbeattime = time.Now().Add(rf.heartbeattimeout)
		DPrintf("%d get a new leader %d", rf.me, args.Id)
		rf.currentTerm = args.Term
		rf.leader = args.Id
		rf.state = follower
		if rf.VoteForTerm > args.Term {
			return
		}
		rf.votedFor = -1
		rf.VoteForTerm = -1
		//DPrintf("%d is still my leader", args.Id)
		//todo

		//rf.commandchannel <- checktimeflag
	} else if rf.state == candidiate && rf.currentTerm == args.Term {
		rf.heartbeattime = time.Now().Add(rf.heartbeattimeout)
		rf.leader = args.Id
		rf.state = follower
		rf.votedFor = -1
		rf.VoteForTerm = -1
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

func (rf *Raft) electtimeticker() {
	rf.mu.Lock()
	rf.electtime = time.Now().Add(time.Duration(1000) * time.Millisecond)
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != candidiate {
			rf.mu.Unlock()
			return
		}

		if rf.electtime.Before(time.Now()) {
			//DPrintf("sad to say you should do it again")
			rf.needelect = true
			rf.cond.Broadcast()
			rf.mu.Unlock()
			return
		}
		rf.cond.Broadcast()
		rf.mu.Unlock()
	}
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
		rf.mu.Lock()

		if rf.state == follower {
			rf.mu.Unlock()
			for !rf.killed() {
				rf.mu.Lock()
				if rf.heartbeattime.Before(time.Now()) {
					rf.mu.Unlock()
					DPrintf("%d timeout", rf.me)
					break
				}
				rf.mu.Unlock()

				//go rf.timeticker()
				//_ = <-rf.commandchannel
				//if kind != checktimeflag && kind != voteflag {
				//	return
				//}
			}

			rf.start_election() //timeout enter the election state

		} else if rf.state == candidiate {
			//if it's killed, stop at once
			rf.needelect = false
			go rf.electtimeticker()
			var voteneeded int

			if len(rf.peers)%2 == 1 {
				voteneeded = ((len(rf.peers) + 1) / 2)
			} else {
				voteneeded = (len(rf.peers) / 2)
			}

			for rf.requestvote < voteneeded && rf.finish != len(rf.peers) && !rf.killed() && rf.state == candidiate && !rf.needelect {
				rf.cond.Wait()
				//DPrintf("%d get %d votes in term %d", rf.me, rf.requestvote, rf.currentTerm)
			}

			if rf.requestvote >= voteneeded && !rf.killed() && rf.state == candidiate && !rf.needelect {
				DPrintf("%d is leader now", rf.me)
				rf.state = leader
				rf.votedFor = -1
				rf.leader = -1
				rf.mu.Unlock()
			} else if !rf.killed() && rf.state == candidiate && rf.needelect {
				rf.mu.Unlock()
				DPrintf("need next election")
				rf.start_election()
			}

		} else {
			rf.mu.Unlock()
			go rf.sendHeartbeatPeriodically()
			for !rf.killed() {
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			}

			//todo
		}

	}
}

func (rf *Raft) sendHeartbeatPeriodically() {
	DPrintf("%d send heartbeat", rf.me)
	r := 100

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		for num, _ := range rf.peers {
			rf.mu.Lock()
			term, id := rf.currentTerm, rf.me

			if rf.killed() || rf.state != leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if num != rf.me {
				go func(num int) {
					request := EasyMes{Term: term, Id: id}
					reply := EasyMes{}

					if ok := rf.sendHeartbeat(num, &request, &reply); !ok {
						//DPrintf("i'm leader %d,opps, server %d seems to be down", rf.me, num)
					}
				}(num)
			}
		}
		time.Sleep(time.Duration(r) * time.Millisecond)
	}
}

//send the RequestVote in parallel, not count the votes

func (rf *Raft) start_election() {
	rf.mu.Lock()
	rf.state = candidiate
	if rf.VoteForTerm != -1 {
		rf.currentTerm = rf.VoteForTerm + 1
	} else {
		rf.currentTerm++
	}
	rf.requestvote = 1
	rf.finish = 1
	rf.votedFor = rf.me
	rf.leader = -1
	DPrintf("%d start to elect in term %d", rf.me, rf.currentTerm)
	term, id := rf.currentTerm, rf.me
	rf.mu.Unlock()

	for num, _ := range rf.peers {
		//turn off the server at once
		rf.mu.Lock()
		if rf.killed() || rf.state != candidiate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if num != rf.me {
			go func(num int) {
				request := RequestVoteArgs{Term: term, CandidateId: id}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(num, &request, &reply)
				//DPrintf("%d don't vote for %d in term %d", num, rf.me ,reply.Term)
				if ok {
					rf.mu.Lock()
					rf.finish++
					if reply.VoteGranted && reply.Term == rf.currentTerm {
						rf.requestvote++
						rf.cond.Broadcast()
						rf.mu.Unlock()
						return
					}
					rf.cond.Broadcast()
					rf.mu.Unlock()
				}
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
	timeout := time.Duration(rand.Int63n(200)+300) * time.Millisecond //timeout wait for heartbeat
	rf := &Raft{currentTerm: 0, state: follower, heartbeattimeout: timeout, heartbeattime: time.Now().Add(timeout), leader: -1, votedFor: -1, VoteForTerm: -1}
	rf.commandchannel = make(chan int)
	rf.cond = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.heartbeattime = time.Now().Add(timeout)
	DPrintf("%d initialize", rf.me)
	go rf.ticker()

	return rf
}
