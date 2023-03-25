package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

var (
	ErrTaskNotFound = errors.New("task not found")
)

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
)

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusAssigned
	TaskStatusFinish
)

type Task struct {
	Idx    int
	Files  []string
	Type   Phase
	Status TaskStatus
}

type WorkerState int

const (
	WorkerStateWait WorkerState = iota
	WorkerStateReady
	WorkerStateFinish
)

type Coordinator struct {
	mu      sync.Mutex
	nReduce int
	phase   Phase
	tasks   []Task
	finish  bool
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.finish {
		reply.State = WorkerStateFinish
		return nil
	}

	if idx, ok := c.currentPendingTask(); ok {
		c.tasks[idx].Status = TaskStatusAssigned
		reply.NReduce = c.nReduce
		reply.Task = c.tasks[idx]
		reply.State = WorkerStateReady
	} else {
		return ErrTaskNotFound
	}

	if _, ok := c.currentPendingTask(); !ok {
		if c.phase == PhaseMap {
			c.phase = PhaseReduce
		} else if c.phase == PhaseReduce {
			c.finish = true
		}
	}

	if args.PrevTask.Idx >= 0 {
		idx := args.PrevTask.Idx
		if _, ok := c.find(idx, args.PrevTask.Type); ok {
			c.tasks[idx].Files = args.PrevTask.Files
			c.tasks[idx].Type = args.PrevTask.Type
			c.tasks[idx].Status = args.PrevTask.Status
		}
	}

	if len(args.NewReduceFiles) > 0 {
		for idx, file := range args.NewReduceFiles {
			taskIdx, ok := c.find(idx, PhaseReduce)
			if ok && file != "" {
				c.tasks[taskIdx].Files = append(c.tasks[taskIdx].Files, file)
			}
		}
	}

	return nil
}

func (c *Coordinator) currentPendingTask() (int, bool) {
	for idx, task := range c.tasks {
		if task.Status == TaskStatusPending && task.Type == c.phase {
			return idx, true
		}
	}
	return 0, false
}

func (c *Coordinator) find(taskIdx int, taskType Phase) (int, bool) {
	for idx, task := range c.tasks {
		if task.Idx == taskIdx && task.Type == taskType {
			return idx, true
		}
	}
	return 0, false
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

	return c.finish
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		phase:   PhaseMap,
		finish:  false,
	}

	// init map tasks
	for idx, file := range files {
		c.tasks = append(c.tasks, Task{
			Idx:    idx,
			Files:  []string{file},
			Type:   PhaseMap,
			Status: TaskStatusPending,
		})
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.tasks = append(c.tasks, Task{
			Idx:    i,
			Files:  []string{},
			Type:   PhaseReduce,
			Status: TaskStatusPending,
		})
	}

	c.server()
	return &c
}
