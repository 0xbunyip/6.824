package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	mapMaster    *MapMaster
	reduceMaster *ReduceMaster
}

type MapMaster struct {
	mapTasks []*MapTask
	nMapDone int
	lock     sync.RWMutex
}

type MapTask struct {
	filename string
	status   TaskStatus
}

type ReduceMaster struct {
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// TODO: allow worker to start shuffling phase as soon as first map finished
	if task, ok := m.mapMaster.BookIdleTask(); ok {
		// Map phase
		reply.Filename = task.filename
		reply.IsMap = true
	} else {
	}
	return nil
}

func (m *MapMaster) BookIdleTask() (MapTask, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, task := range m.mapTasks {
		if task.status != Idle {
			continue
		}

		task.status = InProgress // TODO: timeout task after 10s
		return *task, true
	}
	return MapTask{}, false
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

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := make([]*MapTask, len(files))
	for i, file := range files {
		mapTasks[i] = &MapTask{
			filename: file,
			status:   Idle,
		}
	}

	m := Master{
		mapMaster: &MapMaster{
			mapTasks: mapTasks,
			nMapDone: 0,
			lock:     sync.RWMutex{},
		},
		reduceMaster: &ReduceMaster{},
	}

	// Your code here.

	m.server()
	return &m
}
