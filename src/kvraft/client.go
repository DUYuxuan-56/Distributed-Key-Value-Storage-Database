package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    clientId int
    lastLeader int
    seq int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    
    ck.clientId=nrand()
    ck.lastLeader=0
    ck.seq=0
    
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
type GetReply struct {
    Value  string
    IsLeader bool
}
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    rf.mu.Lock()
    args:=GetArgs{
        Key: key
        ClientId: ck.clientId
        Op: "Get"
        Seq:ck.seq
    }
    var reply GetReply
    rf.mu.Unock()
    for {
        doneCh := make(chan bool, 1)
        go func(){
            ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)
            doneCh<- ok
        }()
        select{
            case<-time.After(600 * time.Millisecond):
                DPrintf("clerk(%d) retry get after timeout\n", ck.clientId)
                continue
            case ok := <- doneCh:
                if reply.IsLeader==true{
                    ck.seq++
                    return reply.Value
                }else{
                    //try next
                    rf.mu.Lock()
                    ck.lastLeader++
                    ck.lastLeader%=len(ck.servers)
                    rf.mu.Unock()
                    continue
                }
        }
        
    }
    
    
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    rf.mu.Lock()
    args:=PutAppendArgs{
        Key : key
        Value : value
        ClientId : ck.clientId
        Op: op
        Seq:ck.seq
    }
    var reply PutAppendReply
    rf.mu.Unock()
    for {
        doneCh := make(chan bool, 1)
        go func(){
            ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)
            doneCh<- ok
        }()
        select{
            case<-time.After(600 * time.Millisecond):
                DPrintf("clerk(%d) retry put after timeout\n", ck.clientId)
                continue
            case ok := <- doneCh:
                if reply.IsLeader==true{
                    ck.seq++
                    return
                }else{
                    //try next
                    rf.mu.Lock()
                    ck.lastLeader++
                    ck.lastLeader%=len(ck.servers)
                    rf.mu.Unock()
                    continue
                }
        }
        
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
