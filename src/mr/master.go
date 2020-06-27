package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mapMaster    *MapMaster
	reduceMaster *ReduceMaster
}

type MapMaster struct {
	tasks       []*MasterMapTask
	nMapDone    int
	nReduce     int
	reduceInput chan MasterReduceInput // to send map output files
	lock        sync.RWMutex
}

type MasterMapTask struct {
	filename string
	status   TaskStatus
	id       int
	nReduce  int
}

type MasterReduceTask struct {
	filenames []string
	status    TaskStatus
	id        int
}

type ReduceMaster struct {
	inputs chan MasterReduceInput
	tasks  map[int]*MasterReduceTask
	lock   sync.RWMutex
}

type MasterReduceInput struct {
	ids       []int // partition id
	filenames []string
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
	if !m.mapMaster.AllDone() {
		// Map phase
		reply.Exit = false
		if task, ok := m.mapMaster.BookIdleTask(); ok {
			reply.Filenames = []string{task.filename}
			reply.IsMap = true
			reply.NumReduce = task.nReduce
			reply.ID = task.id
		} else {
			return fmt.Errorf("no more map task, please wait")
		}
	} else if !m.reduceMaster.AllDone() {
		reply.Exit = false
		if task, ok := m.reduceMaster.BookIdleTask(); ok {
			reply.Filenames = task.filenames
			reply.IsMap = false
			reply.ID = task.id
		} else {
			return fmt.Errorf("no more reduce task, please wait")
		}
	} else {
		reply.Exit = true
	}
	return nil
}

func (m *Master) NotifyTaskComplete(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	if args.IsMap {
		m.mapMaster.MarkTaskComplete(args.ID, args.OutputFiles)
	} else {
		m.reduceMaster.MarkTaskComplete(args.ID, args.OutputFiles)
	}
	return nil
}

// BookIdleTask returns an idle map task if there's one available
// It also indicates whether all map tasks are completed
// Otherwise, it returns an empty task
func (m *MapMaster) BookIdleTask() (MasterMapTask, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i, task := range m.tasks {
		if task.status != Idle {
			continue
		}

		task.status = InProgress
		task.id = i
		task.nReduce = m.nReduce
		go waitForTask(task)
		return *task, true
	}
	return MasterMapTask{}, false
}

func (m *MapMaster) AllDone() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, task := range m.tasks {
		if task.status != Completed {
			return false
		}
	}
	return true
}

func (m *MapMaster) MarkTaskComplete(id int, files map[int]string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	log.Printf("map task %d completed, num output file: %v", id, len(files))
	m.tasks[id].status = Completed
	input := MasterReduceInput{}
	for rid, filename := range files {
		input.filenames = append(input.filenames, filename)
		input.ids = append(input.ids, rid)
	}
	m.reduceInput <- input
}

func (m *ReduceMaster) process() {
	for {
		input := <-m.inputs
		m.lock.Lock()
		for i, id := range input.ids {
			if _, ok := m.tasks[id]; !ok {
				m.tasks[id] = &MasterReduceTask{
					filenames: nil,
					status:    Idle,
					id:        id,
				}
			}
			m.tasks[id].filenames = append(m.tasks[id].filenames, input.filenames[i])
		}
		m.lock.Unlock()
	}
}

func (m *ReduceMaster) BookIdleTask() (MasterReduceTask, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, task := range m.tasks {
		if task.status != Idle {
			continue
		}

		task.status = InProgress
		go waitForTask(task)
		return *task, true
	}
	return MasterReduceTask{}, false
}

func (m *ReduceMaster) MarkTaskComplete(id int, file map[int]string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.tasks[id].status = Completed
	log.Printf("reduce task %d completed, output file: %v", id, file[id])
}

func (m *ReduceMaster) AllDone() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, task := range m.tasks {
		if task.status != Completed {
			return false
		}
	}
	return true
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
	// Your code here.
	ret := m.mapMaster.AllDone() && m.reduceMaster.AllDone()
	if ret {
		log.Println("all done, exiting...")
		time.Sleep(3 * time.Second)
	}

	return ret
}

type InProgressTask interface {
	Status() TaskStatus
	ID() int
	SetStatus(s TaskStatus)
}

func (task *MasterMapTask) Status() TaskStatus {
	return task.status
}

func (task *MasterMapTask) ID() int {
	return task.id
}

func (task *MasterMapTask) SetStatus(s TaskStatus) {
	task.status = s
}

func (task *MasterReduceTask) Status() TaskStatus {
	return task.status
}

func (task *MasterReduceTask) SetStatus(s TaskStatus) {
	task.status = s
}

func (task *MasterReduceTask) ID() int {
	return task.id
}

func waitForTask(task InProgressTask) {
	time.Sleep(10 * time.Second)
	if task.Status() == InProgress {
		log.Printf("task %d exceed deadline, put it back to queue", task.ID())
		task.SetStatus(Idle)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	tasks := make([]*MasterMapTask, len(files))
	for i, file := range files {
		tasks[i] = &MasterMapTask{
			filename: file,
			status:   Idle,
		}
	}

	reduceMaster := &ReduceMaster{
		inputs: make(chan MasterReduceInput, 100),
		tasks:  make(map[int]*MasterReduceTask),
		lock:   sync.RWMutex{},
	}
	go reduceMaster.process()

	m := Master{
		mapMaster: &MapMaster{
			tasks:       tasks,
			nMapDone:    0,
			lock:        sync.RWMutex{},
			nReduce:     nReduce,
			reduceInput: reduceMaster.inputs,
		},
		reduceMaster: reduceMaster,
	}

	// Your code here.

	m.server()
	return &m
}
