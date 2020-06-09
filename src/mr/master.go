package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type Master struct {
	// Your definitions here.
	NumWorker int
	WorkerState []string//idle,inProgress,completed,failed
    ReduceWorkerState []string
    job map[int]string
    ReduceJob map[int]int
    timers map[int] 
}
id:=-1
mu sync.Mutex
// Your code here -- RPC handlers for the worker to call.
func (m *Master) ForTask(args *Args, reply *Reply) []string {

	if args.id==-1&&GlobalTimer<10 * time.Second{
        id=id+1
        reply.id=id
        mu.lock()
        m.WorkerState[id]="idle"
        mu.unlock()
        go func (id int) {
            timers[id] := time.NewTimer(10 * time.Second)

        }(id)
        /*time.Sleep(2 * time.Second)
        stop := timer.Stop()
        if stop {

        }*/
        return
	}else if args.id==-1&&GlobalTimer>10 * time.Second{
        return
    }
    else {
        mu.lock()
        m.timers[args.id] = time.Reset(10 * time.Second)
        mu.unlock()
        if NumWorker<0{
          NumWorker=id
        }
        if m.WorkerState[args.id]="failed"{
          return
        }
        if m.job[args.id] first assigned or overwritten{
          reply.DocList=m.job[args.id]
          mu.lock()
          m.WorkerState[args.id]="progress"
          mu.unlock()
          return
        }else {
          reply=nil
          return
        }
    }
	
	return nil
}
func (m *Master) MapDone(args *Args, reply *Reply) nil {
	if m.timer[args.id]<10{
        mu.lock()
        m.job[args.id]="completed"
        mu.unlock()
    }else{
        reply.terminate=true
    }
    AllCompleted:=true
    for key, element := range m.WorkerState{
        if element!="completed"&&element!="failed"{
            AllCompleted=false
        }
    }
    if AllCompleted==true{
        reply.AppReduce=true
        return
    }else{
        for key, element := range m.WorkerState{
          if element=="progress"&&m.timer[key]>=10 * time.Second{
            //search for available workers
            avail:=-1
            for keyInner, elementInner := range m.WorkerState{
              if elementInner=="idle"||elementInner=="completed"{
                avail=keyInner
              }
            }
            mu.lock()
            if avail!=-1{
              m.job[avail]=m.job[key]
            }
            m.WorkerState[key]="failed"
            mu.unlock()
          }
        }
        reply.AppReduce=false
        return
    }
}
func (m *Master) Reduce(args *Args, reply *Reply) nil {
    mu.lock()
    m.timers[args.id] = time.Reset(10 * time.Second)
    mu.unlock()
    
	if m.ReduceJob[args.id] first assigned or overwritten{
        reply.DocList=m.ReduceJob[args.id]
        mu.lock()
        m.ReduceWorkerState[args.id]="progress"
        mu.unlock()
        return
    }else {
        reply=nil
        return
    }
}

func (m *Master) ReduceDone(args *Args, reply *Reply) nil {
	if m.timer[args.id]<10{
        mu.lock()
        m.ReduceJob[args.id]="completed"
        mu.unlock()
    }else{
        reply.terminate=true
    }
    AllCompleted:=true
    for key, element := range m.ReduceWorkerState{
        if element!="completed"&&element!="failed"{
            AllCompleted=false
        }
    }
    if AllCompleted==true{
        reply.AppTerm=true
        return
    }else{
        for key, element := range m.ReduceWorkerState{
          if element=="progress"&&m.timer[key]>=10 * time.Second{
            //search for available workers
            avail:=-1
            for keyInner, elementInner := range m.ReduceWorkerState{
              if elementInner=="idle"||elementInner=="completed"{
                avail=keyInner
              }
            }
            mu.lock()
            if avail!=-1{
              m.ReduceJob[avail]=m.ReduceJob[key]
            }
            m.ReduceWorkerState[key]="failed"
            mu.unlock()
          }
        }
        reply.AppTerm=false
        return
    }
}
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	//if all assigned-work workers returned

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
    
    m.job := make(map[int]string)
    m.ReduceJob := make(map[int]string)
    GlobalTimer:= time.NewTimer(10 * time.Second)
    
	//split the files into nReduce buckets
	
	for filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}
	
	m.server()
	return &m
}
