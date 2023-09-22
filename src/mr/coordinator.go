package mr

import (
	"6.5840/raft"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mapTasks    []*task
	reduceTasks []*task
	files       []string
	mutex       sync.Mutex
}

type task struct {
	done     bool
	assigned bool
	started  time.Time
	workerId int
}

const taskTimeout = 10 * time.Second

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.MapTasks = len(c.mapTasks)
	reply.ReduceTasks = len(c.reduceTasks)

	mapToWorker := make([]int, len(c.mapTasks))
	mapDone := true
	for id, task := range c.mapTasks {
		if !task.assigned || (!task.done && time.Since(task.started) > taskTimeout) {
			reply.TaskType = Map
			reply.TaskId = id
			reply.Filename = c.files[id]
			task.workerId = args.WorkerId
			task.started = time.Now()
			task.assigned = true
			return nil
		}
		mapDone = mapDone && task.done
		mapToWorker[id] = task.workerId
	}
	if !mapDone {
		raft.DPrintf("waiting for all map tasks to finish before starting reduce tasks")
		reply.TaskType = Wait
		return nil
	}

	reply.MapToWorker = mapToWorker
	reduceDone := true
	for id, task := range c.reduceTasks {
		reduceDone = reduceDone && task.done
		if !task.assigned || (!task.done && time.Since(task.started) > taskTimeout) {
			reply.TaskType = Reduce
			reply.TaskId = id
			task.workerId = args.WorkerId
			task.started = time.Now()
			task.assigned = true
			return nil
		}
	}
	if !reduceDone {
		raft.DPrintf("waiting for all reduce tasks to finish before shutting down")
		reply.TaskType = Wait
		return nil
	}

	return errors.New("job is done")
}

func (c *Coordinator) MarkTask(args *MarkTaskArgs, reply *MarkTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTasks[args.TaskId].done = true
	case Reduce:
		c.reduceTasks[args.TaskId].done = true
	default:
		return fmt.Errorf("unexpected task type: %s", args.TaskType)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	raft.DPrintf("server listening on unix socket: %s", sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, task := range c.mapTasks {
		if !task.done {
			return false
		}
	}
	for _, task := range c.reduceTasks {
		if !task.done {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.mapTasks = make([]*task, len(files))
	for i := range c.mapTasks {
		c.mapTasks[i] = new(task)
	}
	c.reduceTasks = make([]*task, nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i] = new(task)
	}
	c.server()
	return &c
}
