package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
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

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

type Tasker struct {
	nReduce int

	mapf     MapF
	reducef  ReduceF
	tasks    chan WorkerTask
	numTasks int32
}

func NewTasker(mapf MapF, reducef ReduceF) *Tasker {
	t := &Tasker{
		mapf:     mapf,
		reducef:  reducef,
		numTasks: 0,
		tasks:    make(chan WorkerTask, 10),
	}
	go t.process()
	return t
}

func (t *Tasker) process() {
	for {
		// Get a new task
		task := <-t.tasks

		// Work until done
		if task.isMap {
			t.doMap(task)
		} else {
			t.doReduce(task)
		}
	}
}

func (t *Tasker) doMap(task WorkerTask) {
	for _, filename := range task.filenames {
		// Read file
		content, err := readMapInput(filename)
		if err != nil {
			log.Printf("%+v", err)
			continue
		}

		// Do map
		log.Println("performing map of task", task.id)
		kvs := t.mapf(filename, string(content))

		// Output
		out, err := writeMapOutput(task.id, task.nReduce, kvs)

		// Return result to master
		if err == nil {
			notifyMapComplete(task.id, out)
		}
	}
	atomic.AddInt32(&t.numTasks, -1)
}

func notifyMapComplete(id int, outfiles map[int]string) {
	log.Printf("sending map result: id = %v, num files = %v", id, len(outfiles))
	args := &TaskCompletedArgs{
		IsMap:       true,
		ID:          id,
		OutputFiles: outfiles,
	}
	reply := &TaskCompletedReply{}
	call("Master.NotifyTaskComplete", &args, &reply)
}

func (t *Tasker) doReduce(task WorkerTask) {
	// Read intermidiate files
	log.Println("reduce: reading intermediate files for task", task.id)
	kvs := []KeyValue{}
	for _, filename := range task.filenames {
		kv, err := readReduceInput(filename)
		if err != nil {
			log.Println(err)
			continue
		}
		kvs = append(kvs, kv...)
	}

	// Sort by keys to produce sorted output
	sort.Sort(ByKey(kvs))

	// Do reduce
	log.Printf("reduce: performing task %d with %d keys", task.id, len(kvs))
	oname := fmt.Sprintf("mr-out-%d", task.id)
	file, _ := os.Create(oname)
	for i, j := 0, 0; i < len(kvs); i = j {
		values := []string{}
		for ; j < len(kvs); j++ {
			if kvs[j].Key != kvs[i].Key {
				break
			}
			values = append(values, kvs[j].Value)
		}

		output := t.reducef(kvs[i].Key, values)

		// Output
		fmt.Fprintf(file, "%v %v\n", kvs[i].Key, output)
	}
	err := file.Close()

	// Return result to master
	if err == nil {
		notifyReduceComplete(task.id, map[int]string{
			task.id: oname,
		})
	}

	atomic.AddInt32(&t.numTasks, -1)
}

func notifyReduceComplete(id int, outfiles map[int]string) {
	log.Printf("sending reduce result: id = %v, files = %v", id, outfiles)
	args := &TaskCompletedArgs{
		IsMap:       false,
		ID:          id,
		OutputFiles: outfiles,
	}
	reply := &TaskCompletedReply{}
	call("Master.NotifyTaskComplete", &args, &reply)
}

func readMapInput(filename string) (string, error) {
	log.Println("reading map input of file:", filename)
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("cannot open %v: %w", filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return "", fmt.Errorf("cannot read %v: %w", filename, err)
	}
	file.Close()
	return string(content), nil
}

func readReduceInput(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open %v: %w", filename, err)
	}

	kvs := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

func writeMapOutput(mapID, nReduce int, kvs []KeyValue) (map[int]string, error) {
	log.Printf("writing map output: id = %v, len(kvs) = %v", mapID, len(kvs))
	sort.Sort(ByKeyHash{
		kvs: kvs,
		hasher: func(key string) int {
			return ihash(key) % nReduce
		},
	})
	reduceID := -1 // reduce task id of the current output file
	var file *os.File

	out := map[int]string{}
	for _, kv := range kvs {
		// Get correct file
		rid := ihash(kv.Key) % nReduce
		if rid != reduceID {
			// Close old file if needed
			if file != nil {
				file.Close()
			}

			// Open new file
			filename := fmt.Sprintf("mr-%d-%d", mapID, rid)
			file, _ = os.Create(filename)
			reduceID = rid
			out[rid] = filename
		}

		enc := json.NewEncoder(file)
		if err := enc.Encode(&kv); err != nil {
			log.Println(err)
			return nil, err
		}
	}

	if file != nil {
		if err := file.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (t *Tasker) IsIdle() bool {
	return t.numTasks == 0
}

func (t *Tasker) Work(task WorkerTask) {
	atomic.AddInt32(&t.numTasks, 1)
	t.tasks <- task
}

type WorkerTask struct {
	filenames []string
	isMap     bool
	id        int
	nReduce   int
}

func askForTask() (WorkerTask, bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Master.RequestTask", &args, &reply)
	if !ok {
		return WorkerTask{}, false
	}

	if reply.Exit {
		log.Fatal("received exit request, exiting...")
	}

	log.Printf("received a new task: map = %v, id = %v", reply.IsMap, reply.ID)
	return WorkerTask{
		filenames: reply.Filenames,
		isMap:     reply.IsMap,
		id:        reply.ID,
		nReduce:   reply.NumReduce,
	}, true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	tasker := NewTasker(mapf, reducef)
	for {
		// Check current task
		if tasker.IsIdle() {
			// Idling, ask for task
			task, ok := askForTask()
			if ok {
				tasker.Work(task)
			}
		}

		// Sleep to prevent spamming master
		time.Sleep(1 * time.Second)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	log.Println(err)
	return false
}

// for sorting by hash of key
type ByKeyHash struct {
	kvs    []KeyValue
	hasher func(string) int
}

func (a ByKeyHash) Len() int      { return len(a.kvs) }
func (a ByKeyHash) Swap(i, j int) { a.kvs[i], a.kvs[j] = a.kvs[j], a.kvs[i] }
func (a ByKeyHash) Less(i, j int) bool {
	ih := a.hasher(a.kvs[i].Key)
	jh := a.hasher(a.kvs[j].Key)
	if ih != jh {
		return ih < jh
	}
	return a.kvs[i].Key < a.kvs[j].Key
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
