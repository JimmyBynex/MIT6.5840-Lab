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

const (
	mapPhase    coordinatorState = 0
	reducePhase coordinatorState = 1
	donePhase   coordinatorState = 2

	idle       taskState = 0
	inProgress taskState = 1
	completed  taskState = 2
)

type coordinatorState int

type taskState int

type Coordinator struct {
	// Your definitions here.
	currentPhase coordinatorState
	nMap         int
	nReduce      int

	mapFinish    int
	reduceFinish int

	mapTask    []task
	reduceTask []task
	mu         sync.Mutex
}

type task struct {
	state   taskState
	start   int64
	mapFile string //only for mapTask
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.currentPhase {
	case mapPhase:
		for id := range c.mapTask {
			switch c.mapTask[id].state {
			case idle:
				c.mapTask[id].state = inProgress
				c.mapTask[id].start = time.Now().Unix()
				reply.TaskType = mapTask
				reply.File = c.mapTask[id].mapFile
				reply.Id = id
				reply.NReduce = c.nReduce

				c.mapTask[id].state = inProgress
				return nil
			case inProgress:
				cur := time.Now().Unix()
				if cur-c.mapTask[id].start > 10 {
					c.mapTask[id].start = time.Now().Unix()
					reply.TaskType = mapTask
					reply.File = c.mapTask[id].mapFile
					reply.Id = id
					reply.NReduce = c.nReduce
					return nil
				}
			default:
			}
		}
		reply.Wait = true
		return nil
	case reducePhase:
		for id := range c.reduceTask {
			switch c.reduceTask[id].state {
			case idle:
				c.reduceTask[id].state = inProgress
				c.reduceTask[id].start = time.Now().Unix()
				reply.TaskType = reduceTask
				reply.Id = id

				c.reduceTask[id].state = inProgress
				return nil
			case inProgress:
				cur := time.Now().Unix()
				if cur-c.reduceTask[id].start > 10 {
					c.reduceTask[id].start = time.Now().Unix()
					reply.TaskType = reduceTask
					reply.Id = id
					return nil
				}
			default:
			}
		}
		reply.Wait = true
		return nil
	case donePhase:
		reply.TaskType = doneTask
		return nil
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case mapTask:
		c.mapTask[args.Id].state = completed
		c.mapFinish++
		if c.mapFinish == c.nMap {
			c.currentPhase = reducePhase
		}
		return nil
	case reduceTask:
		c.reduceTask[args.Id].state = completed
		c.reduceFinish++
		if c.reduceFinish == c.nReduce {
			c.currentPhase = donePhase
		}
		return nil
	default:
		return errors.New("got wrong finishTask taskType")
	}
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
	ret := false
	c.mu.Lock()
	if c.currentPhase == donePhase {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	nMap := len(files)
	c.currentPhase = mapPhase
	c.nReduce = nReduce
	c.nMap = nMap

	mapTask := make([]task, nMap)
	for id := range mapTask {
		mapTask[id].mapFile = files[id]
		mapTask[id].state = idle
	}

	reduceTask := make([]task, nReduce)
	for i := range reduceTask {
		reduceTask[i].state = idle
	}

	c.mapTask = mapTask
	c.reduceTask = reduceTask
	c.server()
	return &c
}
