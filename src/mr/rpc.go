package mr

import (
	"os"
	"strconv"
)

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	TaskType    TaskType
	TaskId      int
	ReduceTasks int
	MapTasks    int
	Filename    string
	MapToWorker []int
}

type MarkTaskArgs struct {
	TaskType TaskType
	TaskId   int
}

type MarkTaskReply struct {
}

type TaskType string

const Map TaskType = "MAP"
const Reduce TaskType = "REDUCE"
const Wait TaskType = "WAIT"

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
