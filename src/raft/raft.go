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

	"fmt"
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
	currentTerm      int           //当前任期
	votedFor         int           //votefor?
	requestvote      int           //收集到的选票
	cond             sync.Cond     //条件锁
	finish           int           //already send vote request count
	commandchannel   chan int      //in normal state,wait for leader to send the command

	//log        []
}

//all the state uesd for state
const (
	StateType  = iota
	leader     //leader type
	follower   //follower type
	candidiate //candidiate type
)

const (
	noflag = iota
	checktimeflag
	//todo
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

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
	term        int //candidate’s term
	candidateId int //candidate requesting vote
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
	term        int  //currentTerm, for candidate to update itself
	voteGranted bool //true means candidate received vote
}

//2A part
type EasyMes struct {
	term int //candidate’s term
	id   int //candidate requesting vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.currentTerm < args.term {
		rf.currentTerm = args.term
		rf.votedFor = args.candidateId
		rf.state = follower
		reply.voteGranted = true
	} else if rf.currentTerm == args.term && rf.votedFor == args.candidateId {
		reply.voteGranted = true
	} else {
		reply.voteGranted = false
	}
}

func (rf *Raft) Heartbeat(args *EasyMes, reply *EasyMes) {
	if rf.currentTerm == args.term && rf.me == args.id {
		rf.mu.Lock()
		rf.heartbeattime.Add(rf.heartbeattimeout)
		rf.mu.Unlock()
		rf.commandchannel <- checktimeflag
	} else {
		rf.mu.Lock()
		rf.heartbeattime = time.Now()
		rf.mu.Unlock()
		rf.commandchannel <- checktimeflag
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

func (rf *Raft) timeticker() {
	for !rf.killed() && rf.state == follower {
		rf.mu.Lock()
		if rf.heartbeattime.Before(time.Now()) {
			rf.mu.Unlock()
			rf.commandchannel <- checktimeflag
			continue
		}
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
		if rf.state == follower {
			for true {
				rf.mu.Lock()
				if rf.heartbeattime.Before(time.Now()) {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				go rf.timeticker()
				kind := <-rf.commandchannel
				if kind != checktimeflag {
					return
				}
			}

			rf.start_election() //timeout enter the election state

		} else if rf.state == candidiate {
			rf.mu.Lock()
			//if it's killed, stop at once
			for rf.requestvote < (len(rf.peers)/2) && rf.finish != len(rf.peers) && !rf.killed() {
				rf.cond.Wait()
				rf.state = leader
			}
			rf.mu.Unlock()
		} else {
			go rf.sendHeartbeatPeriodically()
			//todo
		}

	}
}

func (rf *Raft) sendHeartbeatPeriodically() {
	r := rand.Int63n(50) + 100
	for rf.state == leader {
		request := EasyMes{term: rf.currentTerm, id: rf.me}
		reply := EasyMes{}
		for num, _ := range rf.peers {
			//turn off the server at once
			if rf.killed() {
				return
			}

			if num != rf.me {
				go func() {
					if ok := rf.sendHeartbeat(num, &request, &reply); !ok {
						fmt.Println("opps, this server seems to be down")
					}
				}()
			}
		}
		time.Sleep(time.Duration(r) * time.Millisecond)
	}
}

//send the RequestVote in parallel, not count the votes

func (rf *Raft) start_election() {
	rf.state = candidiate
	rf.requestvote = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	request := RequestVoteArgs{term: rf.currentTerm, candidateId: rf.me}
	reply := RequestVoteReply{}

	for num, _ := range rf.peers {
		//turn off the server at once
		if rf.killed() {
			return
		}

		if num != rf.me {
			go func() {
				ok := rf.sendRequestVote(num, &request, &reply)
				if ok && reply.voteGranted && reply.term == rf.currentTerm {
					rf.mu.Lock()
					rf.requestvote++
					rf.finish++
					rf.cond.Broadcast()
					rf.mu.Unlock()
				}
			}()
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
	timeout := time.Duration(rand.Int63n(150)+150) * time.Millisecond //timeout wait for heartbeat
	rf := &Raft{currentTerm: 0, state: follower, heartbeattimeout: timeout}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.heartbeattime = time.Now().Add(timeout)
	go rf.ticker()

	return rf
}
