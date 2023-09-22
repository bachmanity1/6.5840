package mr

import (
	"6.5840/raft"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	intermediateFilenameFormat = "mr-intermediate-%d-%d-%d"
	outputFilenameFormat       = "mr-out-%d"
	outputFormat               = "%v %v\n"
	taskWaitDuration           = 2 * time.Second
)

var (
	workerIdLock sync.Mutex
	nextWorkerId int
	workerId     int
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerIdLock.Lock()
	workerId = nextWorkerId
	nextWorkerId++
	workerIdLock.Unlock()
	raft.DPrintf("starting worker %d", workerId)
	for {
		args := GetTaskArgs{}
		args.WorkerId = workerId
		reply := GetTaskReply{}
		hasTask := call("Coordinator.GetTask", &args, &reply)
		if !hasTask {
			break
		}
		raft.DPrintf("got task %s, id: %d", reply.TaskType, reply.TaskId)
		switch reply.TaskType {
		case Map:
			handleMapTask(mapf, &reply)
		case Reduce:
			handleReduceTask(reducef, &reply)
		case Wait:
			time.Sleep(taskWaitDuration)
			continue
		default:
			log.Fatal("unknown task type:", reply.TaskType)
		}

		markTaskArgs := MarkTaskArgs{}
		markTaskArgs.TaskType = reply.TaskType
		markTaskArgs.TaskId = reply.TaskId
		call("Coordinator.MarkTask", &markTaskArgs, &MarkTaskReply{})
	}
}

func handleMapTask(mapf func(string, string) []KeyValue, reply *GetTaskReply) {
	intermediate := make([][]KeyValue, reply.ReduceTasks)
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))

	for _, kv := range kva {
		reduceTaskId := ihash(kv.Key) % reply.ReduceTasks
		intermediate[reduceTaskId] = append(intermediate[reduceTaskId], kv)
	}
	for reduceTaskId, reduceKva := range intermediate {
		intermediateFilename := fmt.Sprintf(intermediateFilenameFormat, workerId, reply.TaskId, reduceTaskId)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFilename)
		}
		defer intermediateFile.Close()

		jsonData, err := json.Marshal(reduceKva)
		if err != nil {
			log.Fatalf("error encoding JSON: %v", err)
		}
		_, err = intermediateFile.Write(jsonData)
		if err != nil {
			log.Fatalf("Error writing to file: %v", err)
		}
	}
}

func handleReduceTask(reducef func(string, []string) string, reply *GetTaskReply) {
	kv := make([]KeyValue, 0)
	for i := 0; i < reply.MapTasks; i++ {
		intermediateFilename := fmt.Sprintf(intermediateFilenameFormat, reply.MapToWorker[i], i, reply.TaskId)
		intermediateFile, err := os.Open(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilename)
		}
		var decodedKv []KeyValue
		decoder := json.NewDecoder(intermediateFile)
		if err := decoder.Decode(&decodedKv); err != nil {
			log.Fatalf("error decoding JSON: %v", err)
		}
		kv = append(kv, decodedKv...)
		if err := intermediateFile.Close(); err != nil {
			log.Fatalf("cannot close %v", intermediateFilename)
		}
	}

	sort.Slice(kv, func(i, j int) bool { return kv[i].Key < kv[j].Key })

	outputFilename := fmt.Sprintf(outputFilenameFormat, reply.TaskId)
	tempOutputFilename := "temp-" + outputFilename
	tempOutputFile, err := os.CreateTemp("", tempOutputFilename)
	if err != nil {
		log.Fatalf("cannot create %v", tempOutputFilename)
	}
	for i := 0; i < len(kv); {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output := reducef(kv[i].Key, values)
		if _, err := fmt.Fprintf(tempOutputFile, outputFormat, kv[i].Key, output); err != nil {
			log.Fatalf("cannot write to %v", tempOutputFilename)
		}

		i = j
	}
	if err := tempOutputFile.Close(); err != nil {
		log.Fatalf("cannot close %v", tempOutputFilename)
	}
	if err := os.Rename(tempOutputFile.Name(), outputFilename); err != nil {
		log.Fatalf("cannot rename %s to %s, err: %v", tempOutputFilename, outputFilename, err)
	}

	// remove intermediate files
	for i := 0; i < reply.MapTasks; i++ {
		intermediateFilename := fmt.Sprintf(intermediateFilenameFormat, reply.MapToWorker[i], i, reply.TaskId)
		if err := os.Remove(intermediateFilename); err != nil {
			log.Fatalf("cannot remove %v", intermediateFilename)
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		return false
	}

	return true
}
