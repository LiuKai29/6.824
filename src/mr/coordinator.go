package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type Task struct {
	ID        int
	FileName  string
	Status    string // "idle", "in-progress", "completed"
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	*reply = TaskReply{TaskID: -1, TaskType: "Wait"}
	mapsCompleted := true
	for i := range c.MapTasks {
		task := &c.MapTasks[i]
		if task.Status == "idle" || (task.Status == "in-progress" && time.Since(task.StartTime) >= 10*time.Second) {
			task.Status = "in-progress"
			task.StartTime = time.Now()
			*reply = TaskReply{TaskID: task.ID, FileName: task.FileName, TaskType: "Map", NMap: len(c.MapTasks), NReduce: c.NReduce}
			return nil
		}
		if task.Status != "completed" {
			mapsCompleted = false
		}
	}
	if !mapsCompleted {
		return nil
	}

	reducesCompleted := true
	for i := range c.ReduceTasks {
		task := &c.ReduceTasks[i]
		if task.Status == "idle" || (task.Status == "in-progress" && time.Since(task.StartTime) >= 10*time.Second) {
			task.Status = "in-progress"
			task.StartTime = time.Now()
			*reply = TaskReply{TaskID: task.ID, FileName: task.FileName, TaskType: "Reduce", NMap: len(c.MapTasks), NReduce: c.NReduce}
			return nil
		}
		if task.Status != "completed" {
			reducesCompleted = false
		}
	}
	if reducesCompleted {
		reply.TaskType = "Exit"
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []Task
	switch args.TaskType {
	case "Map":
		tasks = c.MapTasks
	case "Reduce":
		tasks = c.ReduceTasks
	default:
		return fmt.Errorf("unknown task type %q", args.TaskType)
	}
	tasks[args.TaskID].Status = "completed"
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.MapTasks {
		if task.Status != "completed" {
			return false
		}
	}
	for _, task := range c.ReduceTasks {
		if task.Status != "completed" {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.MapTasks = make([]Task, len(files))
	for i, file := range files {
		c.MapTasks[i] = Task{
			ID:       i,
			FileName: file,
			Status:   "idle",
		}
	}

	c.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			ID:     i,
			Status: "idle",
		}
	}

	c.server()
	return &c
}
