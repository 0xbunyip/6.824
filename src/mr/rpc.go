package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Filenames []string
	IsMap     bool // true for Map task, false for Reduce task
	ID        int
	NumReduce int
	Exit      bool // job finished or something is wrong, worker should exit
}

type TaskCompletedArgs struct {
	IsMap       bool
	ID          int
	OutputFiles map[int]string // reduceID => filename
}

type TaskCompletedReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
