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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
	HeartBeat bool
	Stage     int
	Index     int
}

type TaskReply struct {
	Err           string
	Done          bool
	Wait          bool
	Stage         int // 1: map 2: reduce
	SplitIndex    int // map stage
	SplitFilePath string
	NReduce       int

	ReduceIndex int // reduce stage
	SplitNum    int
}

type ReportTaskArgs struct {
	Stage          int // 1: map 2: reduce
	SplitIndex     int // map stage
	PartitionIndex int // reduce stage
}

type ReportTaskReply struct {
	Err string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
