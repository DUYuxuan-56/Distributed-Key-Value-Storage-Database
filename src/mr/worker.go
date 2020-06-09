package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type TaskArgs struct {
	id int
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//rpc sent
	CallMaster()
	while finished == -1 {
        AskForTask()
        finished=MapDone()
        if finished==-2{//terminate the machine
            return
        }
        time.Sleep(4 * time.Second)
    }
    while finished == -1 {
        AskReduce()
        finished=ReduceDone()
        if finished==-2{//terminate the machine
            return
        }
        time.Sleep(4 * time.Second)
    }
    

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
id=-1
func AskForTask() {
    var args TaskArgs
    args.id=id
    var reply TaskReply
    
	// send the RPC request, wait for the reply.
	call("Master.ForTask", &args, &reply)
    
    if !reply.empty {
        intermediate := []mr.KeyValue{}
        id=reply.id
        DocList=reply.DocList
        for filename := range DocList {
            file, err := os.Open(filename)
            if err != nil {
                log.Fatalf("cannot open %v", filename)
            }
            content, err := ioutil.ReadAll(file)
            if err != nil {
                log.Fatalf("cannot read %v", filename)
            }
            file.Close()
            kva := mapf(filename, string(content))
            intermediate = append(intermediate, kva...)
        }
        sort.Sort(ByKey(intermediate))
        
        i := 0
        for i < len(intermediate) {
            reduceNum:=ihash(intermediate[i].Key)% reply.nReduce
            var oname strings.Builder
            oname.WriteString("mr-%v-%v",id,reduceNum)
            
            if not created {oname, _ := os.Create(oname)}
            
            file, err := os.Open(oname)
            enc := json.NewEncoder(file)
            err := enc.Encode("%v %v\n", intermediate[i].Key, intermediate[i].Value)
            file.Close()
        }
    }
}
func MapDone() int{
    var args MapDoneArgs
    args.id=id
    var reply MapDoneReply
    
	call("Master.MapDone", &args, &reply)
    
    if !reply.empty {
        if reply.terminate == true{
            return -2
        }
        if reply.AppReduce == true{
            return 1 //proceed to reduce
        }else{
            return -1
        }
    }
}
//periodically send this after local mapping done
func AskReduce(){
    var args AskReduceArgs
    args.id=id
    var reply AskReduceReply
    
	MsAlive=call("Master.AskReduce", &args, &reply)
    if MsAlive == false{
        exit
    }
    if !reply.empty {
        DocList:=reply.DocList
        for doc := range DocList{
            intermediate := []mr.KeyValue{}
            var oname strings.Builder
            oname.WriteString("mr-%v-%v",allIds,doc)
            file, err := os.Open(oname)
            dec := json.NewDecoder(file)
            for {
                var kv KeyValue
                if err := dec.Decode(&kv); err != nil {
                  break
                }
                intermediate = append(intermediate, kv)
            }
            file.Close()
            // call Reduce on each distinct key in intermediate[],
            // and print the result to mr-out-.
            var oname strings.Builder
            oname.WriteString("mr-out-")
            oname.WriteString("%v",doc)
            ofile, _ := os.Create(oname)
            
            i := 0
            for i < len(intermediate) {
            
                j := i + 1
                for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
                    j++
                }
                values := []string{}
                for k := i; k < j; k++ {
                    values = append(values, intermediate[k].Value)
                }
                output := reducef(intermediate[i].Key, values)
                
                // this is the correct format for each line of Reduce output.
                fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

                i = j
            }
        }
        
        
    }

}
func ReduceDone(){
    var args ReduceDoneArgs
    args.id=id
    var reply ReduceDoneReply
    
	MsAlive=call("Master.ReduceDone", &args, &reply)
    if MsAlive == false{
        exit
    }
    if !reply.empty {
        if reply.terminate == true{
            return -2
        }
        if reply.AppTerm == true{
            return 1 //proceed to exit
        }else{
            return -1
        }
    }
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
