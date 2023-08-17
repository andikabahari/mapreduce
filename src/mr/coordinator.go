package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusAssigned
	TaskStatusFinish
)

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
)

type Task struct {
	Idx       int
	WorkerId  string
	Filenames []string
	Status    TaskStatus
	Type      TaskType
	TimeStart time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < len(c.mapTasks); i++ {
		task := &c.mapTasks[i]
		if task.Status == TaskStatusPending {
			task.WorkerId = args.WorkerId
			task.Status = TaskStatusAssigned
			reply.NReduce = c.nReduce
			reply.Task = task
			return nil
		}
	}

	for _, task := range c.mapTasks {
		if task.Status != TaskStatusFinish {
			task := &Task{Type: TaskTypeWait}
			reply.NReduce = c.nReduce
			reply.Task = task
			return nil
		}
	}
	for i := 0; i < len(c.reduceTasks); i++ {
		task := &c.reduceTasks[i]
		if task.Status == TaskStatusPending {
			task.WorkerId = args.WorkerId
			task.Status = TaskStatusAssigned
			reply.NReduce = c.nReduce
			reply.Task = task
			return nil
		}
	}

	return errors.New("no pending task")
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, reply *TaskFinishReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Ok = false
	switch args.Type {
	case TaskTypeMap:
		if args.Idx >= 0 && args.Idx < len(c.mapTasks) {
			task := &c.mapTasks[args.Idx]
			task.Status = TaskStatusFinish
			reply.Ok = true
		}
		return nil
	case TaskTypeReduce:
		if args.Idx >= 0 && args.Idx < len(c.reduceTasks) {
			task := &c.reduceTasks[args.Idx]
			task.Status = TaskStatusFinish
			reply.Ok = true
		}
		return nil
	default:
		return errors.New("unknown task type")
	}
}

func (c *Coordinator) NewReduceFilenames(args *NewReduceFilenamesArgs, reply *NewReduceFilenamesReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(args.Filenames) <= c.nReduce {
		for i, filename := range args.Filenames {
			task := &c.reduceTasks[i]
			task.Filenames = append(task.Filenames, filename)
		}
		reply.Ok = true
	} else {
		reply.Ok = false
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.mapTasks {
		if task.Status != TaskStatusFinish {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.Status != TaskStatusFinish {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce}

	// init map tasks
	for i, filename := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Idx:       i,
			Filenames: []string{filename},
			Status:    TaskStatusPending,
			Type:      TaskTypeMap,
		})
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			Idx:       i,
			Filenames: []string{},
			Status:    TaskStatusPending,
			Type:      TaskTypeReduce,
		})
	}

	c.server()
	return &c
}
