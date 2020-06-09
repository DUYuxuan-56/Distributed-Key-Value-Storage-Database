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

import "sync"
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//logEntries

type LogEntry struct {
	Term int
    Command  string 
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
    currentTerm int
    votedFor int
    log  []LogEntry // each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    state State
    
    commitIndex int
    lastApplied int
    
    nextIndex []int
    matchIndex []int
    electionTimer 
    //3b
    snapshottedIndex int
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term=rf.currentTerm
    isLeader=rf.state=="leader"
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

// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int 
    CandidateId int
    LastLogIndex int
    LastLogTerm int

}

// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term   int
    VoteGranted   bool
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
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    reply.Term:=rf.currentTerm
    if args.Term < rf.currentTerm{
        reply.VoteGranted=false
        rf.mu.Unlock()
        return
    }else if args.Term == rf.currentTerm{
        rf.votedFor=args.CandidateId
        reset timer
    }
    else{
        rf.currentTerm=args.Term
        rf.state="follower"
        rf.votedFor=args.CandidateId
        reset timer
    }
    If (rf.votedFor == nil || rf.votedFor==args.CandidateId)&& 
    (args.LastLogTerm>rf.currentTerm || (args.LastLogTerm==rf.currentTerm&&args.LastLogIndex>=len(rf.log))){
        reply.VoteGranted=true
        rf.votedFor=args.CandidateId
        reset rf.timer
    }    
    rf.mu.Unlock()
    
}

type AppendEntriesReply struct {
    Term  int
    Success   bool
}
// example AppendEntries RPC handler.

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    reply.Term:=rf.currentTerm
    if args.Term < rf.currentTerm {
        reply.Success=false
        rf.mu.Unlock()
        return
    }else if args.Term == rf.currentTerm{
        rf.state="follower"
        reset timer
    }else{
        rf.currentTerm=args.Term
        rf.state="follower"
        rf.votedFor=nil
        reset timer
    }
    //heartbeat
    if args.Entries==nil{
        rf.mu.Unlock()
        return
    }
    if args.PrevLogIndex >= len(rf.log){
        return
    }
    if rf.log[args.PrevLogIndex].Term!=args.PrevLogTerm {
        reply.Success=false
        rf.mu.Unlock()
        return
    }
    if rf.log[args.PrevLogIndex+1].Term!=args.Entries.Term {
        rf.log = rf.log[0:args.PrevLogIndex]
        //rf.persist()
    }
    rf.log=append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
    
    If args.leaderCommit > rf.commitIndex{
        
        rf.commitIndex =minInt(args.leaderCommit, len(rf.log)-1)
    }
    reply.Success=true
    rf.mu.Unlock()
    return
}

//RequestVote RPC Call
//
func (rf *Raft) sendRequestVote(server int,term int) bool {
	
    args:=RequestVoteArgs{
        Term: term
        CandidateId:rf.me
    }    
    var reply RequestVoteReply
    ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
    if !ok{
        return false
    }
    //process the reply 
    if rf.state != "candidate" || rf.currentTerm != args.Term {
        return
    }
    if reply.VoteGranted{
        return true
    }
    rf.mu.Lock()
    if reply.Term > rf.currentTerm{
        rf.currentTerm=reply.term
        rf.state="follower"
        rf.votedFor=nil
        reset timer
    }
    rf.mu.Unock()
}

//electionLoop RPC Call

func (rf *Raft) electionLoop(){

    rf.mu.Lock()
    if rf.state == "leader" {
		rf.mu.Unlock()
		return
	}
    rf.state="candidate"
    rf.currentTerm++
    rf.votedFor=rf.me
    //rf.persist()
    
    time.Sleep(rand.Intn(300)*time.Millisecond) //Reset election timer
    
    term:=rf.currentTerm
    numOfPeers=len(rf.peers)
    counter:=1
    finished:=1
    cond:=sync.NewCond(&rf.mu)
    rf.mu.Unlock()
    
    for _,server:=range rf.peers{
        if server == rf.me{
            continue
        }
        go func(server int){
            votes:=rf.sendRequestVote(server,term)
            rf.mu.Lock()
            defer rf.mu.Unlock()
            if votes{
                counter++
            }
            finished++
            cond.Broadcast()
        }(server)
    }
    
    rf.mu.Lock()
    for counter<=numOfPeers/2 && finished!=numOfPeers{
            //wait
            cond.wait()
        }
    }
    if counter >numOfPeers/2 && rf.state=="candidate" && rf.currentTerm==term{
        //become leader
        rf.state="leader"
        for i := 0; i < rf.PeersNum; i++ {
            rf.matchIndex[i] = 0
            rf.nextIndex[i] = len(rf.log)
        }
        //reset timer
    }else{
        return
    }
    rf.mu.Unock()
   
}

//sendAppendEntriesRequest RPC Call

func (rf *Raft) sendAppendEntriesRequest(server int) bool{
    rf.mu.Lock()
    prevLogIndex := rf.nextIndex[server] - 1
    args:=AppendEntriesArgs{
        Term: rf.currentTerm
        LeaderId:rf.me
        PrevLogIndex :prevLogIndex
        PrevLogTerm :rf.log[prevLogIndex].term
        Entries :rf.log[rf.nextIndex[server]:]
        LeaderCommit :rf.commitIndex 
    }    
    var reply AppendEntriesReply
    rf.mu.Unock()
    ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
    if !ok{
        return false
    }
    if rf.state != "leader" || rf.currentTerm != args.Term {
        return
    }   
    //process the reply 
    rf.mu.Lock()
    if reply.Term > rf.currentTerm{
    
        rf.currentTerm=reply.term
        rf.state="follower"
        rf.votedFor=nil
        reset timer
    }
    if reply.success==true{
    
        rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
        rf.nextIndex[server] = rf.matchIndex[server]+1
        N := getMajoritySameIndex(rf.matchIndex)
        if N > commitIndex && log[N].Term == rf.currentTerm{
            rf.commitIndex = N
            DPrintf("%v advance commit index to %v", rf, rf.commitIndex)
        }
    }else{
        
        rf.nextIndex[server]--
        decrement nextIndex and retry//if due to inconsistency
    }
    rf.mu.Unock()
    return reply.success
}

func (rf *Raft) AppendEntriesRequest(){
    
    for _,server:=range rf.peers{
        if server == rf.me{
            continue
        }
        go func(server int){
            success:=rf.sendAppendEntriesRequest(server)
            rf.mu.Lock()
            defer rf.mu.Unlock()
            if success{
                counter++
            }
            finished++
            cond.Broadcast()
        }(server)
    }
    rf.mu.Lock()
    numOfPeers=len(rf.peers)
    for counter<=numOfPeers/2 && finished!=numOfPeers{
            cond.wait()
        }
    }
    if counter >numOfPeers/2 && rf.state=="candidate" && rf.currentTerm==term{
        //commit
        rf.commitIndex++//也可能commit了多个。。。
    }else{
        return
    }
    rf.mu.Unock()
}

func (rf *Raft) applyLoop() {
    for{
        time.Sleep(10*Millisecond)
        rf.mu.Lock()
        for rf.lastApplied<rf.commitIndex{
            rf.lastApplied++
            rf.apply()
        }
        rf.mu.Unock()
    }
}

func (rf *Raft) printRF() string {
    return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
        rf.state, rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.commitIndex, rf.lastApplied)
}

//3B
func (rf *Raft) getRelativeLogIndex(index int) int {
	return index - rf.snapshottedIndex
}
func (rf *Raft) ReplaceLogWithSnapshot(appliedIndex int, kvSnapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if appliedIndex <= rf.snapshottedIndex {
		return
	}
    rf.logs = rf.logs[rf.getRelativeLogIndex(appliedIndex):]
	rf.snapshottedIndex = appliedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), kvSnapshot)
 
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.syncSnapshotWith(i)
	}
}
func (rf *Raft) syncSnapshotWith(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
	}
 
	rf.mu.Unlock()
 
	var reply InstallSnapshotReply
 
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
        
		rf.mu.Unlock()
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
    defer rf.mu.Unock()
	term = rf.currentTerm
    if rf.state=="leader"{
        isLeader = true
    }else{
        isLeader = false
    }
    rf.log = append(rf.log, &logEntry{rf.currentTerm, command})
    
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

	// Your initialization code here (2A, 2B, 2C).
    rf.state="follower"
    rf.currentTerm=0
    rf.votedFor=-1
    for _,server range peers{
        go func(){
            for {
                rf.mu.Lock()
                if rf.state=="follower"{
                    rf.mu.Unlock()
                    continue
                }
                if rf.state=="leader"{
                    rf.mu.Unlock()
                    
                    rf.AppendEntriesRequest()
                    continue
                }
                if rf.state=="candidate"{
                    rf.mu.Unlock()
                    rf.electionLoop()
                    continue
                }
            }
        }()
        
    }
    go rf.applyLoop()
    
    for{
        time.Sleep()
        if rf.state=="leader"{
            entries := LogEntry{command:nil,
                                term:nil}
            rf.AppendEntriesRequest()
        }
    }
    
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
