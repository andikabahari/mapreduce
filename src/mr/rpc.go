package mr

import (
	"os"
	"strconv"
)

type GetTaskArgs struct {
	WorkerId string
}

type GetTaskReply struct {
	NReduce int
	Task    *Task
}

type TaskFinishArgs struct {
	Idx  int
	Type TaskType
}

type TaskFinishReply struct {
	Ok bool
}

type NewReduceFilenamesArgs struct {
	Filenames []string
}

type NewReduceFilenamesReply struct {
	Ok bool
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
