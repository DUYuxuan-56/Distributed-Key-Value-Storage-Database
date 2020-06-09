package kvraft

import (
	"../labgob"
    "fmt"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Op string
    
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    data        map[string]string
    notify      map[int]chan struct{}
    latestReplies map[int64]*LatestReply
    //3B
    appliedRaftLogIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    index, term, Leader := kv.rf.Start(args.Op)
    if !Leader{
        reply.IsLeader=false
        return
    }
    
    //some code avoid repeated requests
    kv.mu.Lock()
    if latestReply, ok := kv.latestReplies[args.Cid]; ok && args.Seq<= latestReply.Seq{
        reply.Value = latestReply.Value
        reply.IsLeader=true
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()
    //
    command := Op{Operation:"Get", Key:args.Key, Cid:args.Cid, Seq:args.Seq}
    index, term, _ := kv.rf.Start(command)
    
    kv.mu.Lock()
    ch := make(chan NotifyMsg, 1)
    kv.notify[index] = ch
    kv.mu.Unlock()
    
    select {
        case <-ch:
            curTerm, isLeader := kv.rf.GetState()
            if curTerm!=term || !isLeader{
                reply.IsLeader=false
            }else {
                kv.mu.Lock()
                if value, ok := kv.db[args.Key]; ok {
                    reply.Value = value
                }
                kv.mu.Unlock()
                reply.IsLeader=true
            }
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    index, term, Leader := kv.rf.Start(args.Op)
    if !Leader{
        reply.IsLeader=false
        return
    }
    //some code avoid repeated requests
    kv.mu.Lock()
    if latestReply, ok := kv.latestReplies[args.Cid]; ok && args.Seq<= latestReply.Seq{
        reply.IsLeader=true
        kv.mu.Unlock()
        return
    }
    kv.mu.Unlock()
    //
    command := Op{Operation:"PutAppend", Key:args.Key, Cid:args.Cid, Seq:args.Seq}
	index, term, _ := kv.rf.Start(command)
    
    kv.mu.Lock()
    ch := make(chan NotifyMsg, 1)
    kv.notify[index] = ch
    kv.mu.Unlock()
    
    select {
        case <-ch:
            curTerm, isLeader := kv.rf.GetState()
            if curTerm!=term || !isLeader{
                reply.IsLeader=false
            }else {
                kv.mu.Lock()
                kv.data[args.Key]=args.Value
                kv.mu.Unlock()
                reply.IsLeader=true
            }
    }
    
}
func (kv *KVServer) applyDaemon()  {
    for appliedEntry := range kv.applyCh {
        command := appliedEntry.Command.(Op)
        kv.mu.Lock()
        if latestReply, ok := kv.latestReplies[command.Cid]; !ok || command.Seq > latestReply.Seq {
            switch command.Operation {
                case "Get":
                    latestReply := LatestReply{Seq:command.Seq,}
                    var reply GetReply
                    if value, ok := kv.data[command.Key]; ok {
                        reply.Value = value
                    } else {
                        reply.Err = ErrNoKey
                    }
                    latestReply.Reply = reply
                    kv.latestReplies[command.Cid] = &latestReply
                case "Put":
                    kv.db[command.Key] = command.Value
                    latestReply := LatestReply{Seq:command.Seq}
                    kv.latestReplies[command.Cid] = &latestReply
                case "Append":
                    kv.db[command.Key] += command.Value
                    latestReply := LatestReply{Seq:command.Seq}
                    kv.latestReplies[command.Cid] = &latestReply
                default:
                    panic("invalid command operation")
        }
        DPrintf("%d applied index:%d, cmd:%v\n", kv.me, appliedEntry.CommandIndex, command)
        if ch, ok := kv.notify[appliedEntry.CommandIndex]; ok && ch != nil {
            DPrintf("%d notify index %d\n",kv.me, appliedEntry.CommandIndex)
			close(ch)
			delete(kv.notify, appliedEntry.CommandIndex)
        }
        kv.mu.Unlock()
    }
}
//
func (kv *KVServer) shouldTakeSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
 
	if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		return true
	}
 
	return false
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.latestReplies)
	appliedRaftLogIndex := kv.appliedRaftLogIndex
	kv.mu.Unlock()
 
	kv.rf.ReplaceLogWithSnapshot(appliedRaftLogIndex, w.Bytes())
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
    applyDaemon()
    
	return kv
}
