package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskArgs{}
		reply := TaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			return
		}

		var err error
		switch reply.TaskType {
		case "Map":
			err = runMapTask(&reply, mapf)
		case "Reduce":
			err = runReduceTask(&reply, reducef)
		case "Wait":
			time.Sleep(time.Second)
			continue
		case "Exit":
			return
		default:
			return
		}
		if err != nil {
			return
		}
		completed := TaskDoneArgs{TaskID: reply.TaskID, TaskType: reply.TaskType}
		if !call("Coordinator.CompleteTask", &completed, &TaskDoneReply{}) {
			return
		}
	}
}

func runMapTask(task *TaskReply, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		return err
	}
	if task.NReduce <= 0 {
		return fmt.Errorf("invalid reduce count %d", task.NReduce)
	}
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.FileName, string(content)) {
		reduceID := ihash(kv.Key) % task.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}
	for reduceID, bucket := range buckets {
		file, err := os.CreateTemp(".", "mr-map-*")
		if err != nil {
			return err
		}
		defer os.Remove(file.Name())
		writer := bufio.NewWriter(file)
		encoder := json.NewEncoder(writer)
		for _, kv := range bucket {
			if err := encoder.Encode(&kv); err != nil {
				file.Close()
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, reduceID)
		if err := os.Rename(file.Name(), filename); err != nil {
			return err
		}
	}
	return nil
}

func runReduceTask(task *TaskReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(bufio.NewReader(file))
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				file.Close()
				return err
			}
			intermediate = append(intermediate, kv)
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})
	file, err := os.CreateTemp(".", "mr-reduce-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	writer := bufio.NewWriter(file)
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(writer, "%v %v\n", intermediate[i].Key, output); err != nil {
			return err
		}
		i = j
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", task.TaskID))
}

// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
