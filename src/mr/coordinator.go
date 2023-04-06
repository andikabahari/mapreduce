package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseWait
	PhaseShutdown
)

type Status int

const (
	StatusPending Status = iota
	StatusAssigned
	StatusFinish
)

type Task struct {
	Idx       int
	Files     []string
	Status    Status
	Phase     Phase
	TimeStart time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
	mapCount    int
	reduceCount int
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.PrevTask.Idx >= 0 {
		switch args.PrevTask.Phase {
		case PhaseMap:
			t := &c.mapTasks[args.PrevTask.Idx]
			if t.Status == StatusAssigned {
				t.Status = StatusFinish
				c.mapCount--
			}

			for i, name := range args.NewReduceFiles {
				if name != "" {
					c.reduceTasks[i].Files = append(c.reduceTasks[i].Files, name)
				}
			}
		case PhaseReduce:
			t := &c.reduceTasks[args.PrevTask.Idx]
			if t.Status == StatusAssigned {
				t.Status = StatusFinish
				c.reduceCount--
			}
		}
	}

	if c.mapCount > 0 {
		idx, ok := pendingTask(c.mapTasks)
		if ok {
			t := &c.mapTasks[idx]
			t.Status = StatusAssigned
			t.TimeStart = time.Now()
			reply.NewTask = *t
			reply.NReduce = c.nReduce
		} else {
			reply.NewTask = Task{Phase: PhaseWait}
		}
	} else if c.reduceCount > 0 {
		idx, ok := pendingTask(c.reduceTasks)
		if ok {
			t := &c.reduceTasks[idx]
			t.Status = StatusAssigned
			t.TimeStart = time.Now()
			reply.NewTask = *t
			reply.NReduce = c.nReduce
		} else {
			reply.NewTask = Task{Phase: PhaseWait}
		}
	} else {
		reply.NewTask = Task{Phase: PhaseShutdown}
	}

	return nil
}

var waitDuration time.Duration = -10 * time.Second

func pendingTask(tasks []Task) (int, bool) {
	timeWait := time.Now().Add(waitDuration)
	for i := range tasks {
		if tasks[i].TimeStart.Before(timeWait) && tasks[i].Status == StatusPending {
			return i, true
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

	return c.mapCount == 0 && c.reduceCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		mapCount:    len(files),
		reduceCount: nReduce,
	}

	// init map tasks
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Idx:    i,
			Files:  []string{file},
			Status: StatusPending,
			Phase:  PhaseMap,
		})
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			Idx:    i,
			Files:  []string{},
			Status: StatusPending,
			Phase:  PhaseReduce,
		})
	}

	c.server()
	return &c
}
