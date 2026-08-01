package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type taskType int

const (
	mapTask    taskType = 0
	reduceTask taskType = 1
	doneTask   taskType = 2
)

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

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType taskType
	Wait     bool
	Id       int
	File     string
	NReduce  int
}

type FinishTaskArgs struct {
	TaskType taskType
	Id       int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
