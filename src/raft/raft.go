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
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2

	ELECTION_TIMEOUT_BASE  = 150
	ELECTION_TIMEOUT_RANGE = 150

	HEARTBEAT = 15
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	isLeader  bool
	killed    bool

	electionTimer *time.Timer
	leaderFound   chan int

	// Persistent Data
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Data
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile State Data of Leader
	nextIndex  []int // for each server, the index of the next log entry to be sent to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

// Log Entry object
type LogEntry struct {
	term    int // term in which the entry was created
	command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if rf.persister.RaftStateSize() == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateId  int // Id of Candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// Vote RPC reply structure.
type RequestVoteReply struct {
	Term        int // current term so the candidate can update itself
	VoteGranted bool
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Println(strconv.Itoa(args.CandidateId) + " requested " + strconv.Itoa(rf.me))
	rf.resetTimer()
	reply.Term = rf.currentTerm
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else if rf.currentTerm == args.Term {
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		} else if rf.votedFor == -1 {
			// Ambiguity : May need to use commit Index rather than
			// length of the log, and may need to account for the term
			if len(rf.log) <= args.LastLogIndex {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			} else {
				reply.VoteGranted = false
			}
		}
	} else {
		if len(rf.log) <= args.LastLogIndex {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	}
	//fmt.Println(strconv.Itoa(rf.me) + " voted for " + strconv.Itoa(rf.votedFor))
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC argument structure
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // Id of leader so the follower can redirect (may not be necessary)
	PrevLogIndex int // index of log entry preceding new entries
	PrevLogTerm  int // term of prevLogIndex log entry
	//Entries      []LogEntry
	LeaderCommit int // commitIndex of the leader
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // current term for leader to update itself
	Success bool // true if follower contained an entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: Log replication
	// Currently intereprets AppendEntries as a heart beat and resets the election timeout timer

	// Ignore Entries from outdated/unqualified leader
	if args.Term < rf.currentTerm {
		return
	}

	//s := strconv.Itoa(rf.me) + " is in term " + strconv.Itoa(rf.currentTerm) + " and " + strconv.Itoa(args.LeaderId) + " is in term " + strconv.Itoa(args.Term)
	//fmt.Println(s)
	rf.resetTimer()

	// TODO: verify the leader is valid before announcing the leader is found
	// Empty the leaderFound channel then send leader ID

	//rf.mu.Lock()
	rf.clearLeaderFoundChan()
	rf.leaderFound <- args.LeaderId
	//rf.mu.Unlock()

	reply.Term = rf.currentTerm
}

// Reset the Election Timeout timer
func (rf *Raft) resetTimer() {
	rf.clearTimer()

	// Randomize the election timeout between 150 and 300 ms
	newTimeout := rand.Int63n(ELECTION_TIMEOUT_RANGE) + ELECTION_TIMEOUT_BASE
	rf.electionTimer.Reset(time.Millisecond * time.Duration(newTimeout))
}

func (rf *Raft) clearTimer() {
	// Flush the timer if it was already fired
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
			// Flushing the channel
		default:
			// To prevent a deadlock
			// if the channels value
			// has been consumed by another thread
		}
	}
}

func (rf *Raft) clearLeaderFoundChan() {
	select {
	case <-rf.leaderFound:
		// Flushing the channel
	default:
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.killed = true
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}

	rf.leaderFound = make(chan int)
	// Instantiate an election timer and clear it
	// for future use
	rf.electionTimer = time.NewTimer(time.Second * time.Duration(10))
	rf.clearTimer()

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.log != nil {
		rf.commitIndex = len(rf.log) - 1
	} else {
		rf.log = make([]LogEntry, 0)
	}
	// TODO: lastApplied index yet to be set

	go rf.startRaftInstance()
	return rf
}

// Responsible for managing the state of the raft instance,
// will change state as needed
// 0 - Leader, 1 - Candidate, 2 - Follower
func (rf *Raft) startRaftInstance() {
	nextState := rf.enterFollowerState()
	for {
		if rf.killed {
			return
		}
		switch nextState {
		case LEADER:
			nextState = rf.enterLeaderState()
		case CANDIDATE:
			nextState = rf.enterCandidateState()
		case FOLLOWER:
			nextState = rf.enterFollowerState()
		}
	}
}

// The Raft server instance will enter into leader state and begin
// all of its responsibilties of handling user requests and log replication
func (rf *Raft) enterLeaderState() int {
	rf.isLeader = true
	fmt.Println(strconv.Itoa(rf.me) + " is leader for term " + strconv.Itoa(rf.currentTerm))
	// TODO: Remaining args to be assigned for log replication

	rf.sendHeartBeats()
	heartBeatTicker := time.NewTicker(time.Millisecond * HEARTBEAT)

	for {
		if rf.killed {
			return FOLLOWER
		}

		select {
		case <-heartBeatTicker.C:
			go rf.sendHeartBeats()
		case <-rf.leaderFound:
			// If a peer becomes the new leader, should enter
			// follower state
			return FOLLOWER
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(server, args, reply)
		}(i)
	}
}

// The Raft server instance will enter into follower state and wait
// for AppendEntries and it will start a new election if no
// heartbeat is received within the Election Timeout time
func (rf *Raft) enterFollowerState() int {
	rf.isLeader = false
	rf.resetTimer()

	// If the Election Timeout fires then enter candidate state
	<-rf.electionTimer.C
	//fmt.Println("Follower timed out " + strconv.Itoa(rf.me))
	return CANDIDATE
}

// The Raft server instance will enter into candidate state
// start a new election and request peers for votes
// and watch for AppendEntries from an active leader and
// keep track of an election timeout for this term
func (rf *Raft) enterCandidateState() int {
	var exitElection = make(chan bool)
	var leaderSelected = make(chan bool)
	rf.resetTimer()
	rf.clearLeaderFoundChan()
	go rf.startNewElection(leaderSelected, exitElection)

	for {
		select {
		case <-leaderSelected:
			// This raft instance is the new leader
			exitElection <- true
			return LEADER
		case <-rf.electionTimer.C:
			// Exit current election and start new election
			// fmt.Println(strconv.Itoa(rf.me) + " election timeout")
			exitElection <- true
			return CANDIDATE
		case <-rf.leaderFound:
			// A qualified leader is found, become a follower
			exitElection <- true
			return FOLLOWER
		}
	}
}

// Starts a new election, the Raft server will request all of its peers
// for votes, if it receives a majority of the votes then it will become the leader.
// The Raft server may receive AppendEntries during this process, if it determines
// that another raft server is the leader for the particular term, this process
// should be halted immediately.
func (rf *Raft) startNewElection(leaderSelected, exitChan chan bool) {
	//fmt.Println(strconv.Itoa(rf.me) + " starting a new election with term " + strconv.Itoa(rf.currentTerm+1))
	rf.currentTerm++
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log)
	if len(rf.log) != 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].term
	}

	/* Start new election, acquire votes from peers */
	// Votes for itself
	rf.votedFor = rf.me
	votes := 1

	totalResponses := 1

	var mu sync.Mutex
	var exit bool
	majority := (len(rf.peers) / 2) + 1

	// Request Vote from each peer excluding itself.
	// If an AppendEntry is received from a leader of
	// term greater than equal to its own then this
	// raft instance should go into follower state
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//fmt.Println(strconv.Itoa(rf.me) + " is sending request to " + strconv.Itoa(i))
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			if exit || !ok {
				return
			}

			if reply.VoteGranted {
				mu.Lock()
				votes++
				totalResponses++
				mu.Unlock()
				if votes >= majority {
					leaderSelected <- true
				}
			} else {
				// TODO: if vote isn't granted
				// decide what should be done
				mu.Lock()
				totalResponses++
				mu.Unlock()
				if totalResponses == len(rf.peers) {
					exitChan <- true
				}
			}
		}(i)
	}

	exit = <-exitChan
}
