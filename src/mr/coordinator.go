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
	PENDING uint8 = iota
	RUNNING
	COMPLETE
)

type task struct {
	taskid   int
	state    uint8
	tasktype uint8
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	mtasks  []*task
	rtasks  []*task
	taskch  chan *task
	rm      sync.Mutex
	rcond   *sync.Cond
	mleft   int
	rleft   int
	nreduce int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(req *EmptyMsg, rsp *TaskRsp) error {
	c.rm.Lock()
	done := (c.rleft == 0)
	c.rm.Unlock()
	if done {
		rsp.TaskType = 0
		return nil
	}
	timer := time.NewTimer(time.Second * 10)
	var oneTask *task
	select {
	case oneTask = <-c.taskch:
		c.rm.Lock()
		defer c.rm.Unlock()
		if oneTask.state != PENDING {
			return errors.New("no task")
		}
		oneTask.state = RUNNING
		if oneTask.tasktype == 1 {
			rsp.TaskId, rsp.TaskType = oneTask.taskid, oneTask.tasktype
			rsp.FileName = c.files[oneTask.taskid]
			rsp.CommonArg = c.nreduce
		} else {
			rsp.TaskId, rsp.TaskType = oneTask.taskid, oneTask.tasktype
			rsp.CommonArg = len(c.files)
		}
	case <-timer.C:
		return errors.New("GetTask timeout")
	}

	go func(t *task) {
		time.Sleep(time.Second * 10)
		c.rm.Lock()
		if t.state != COMPLETE {
			t.state = PENDING
			c.rm.Unlock()
			c.taskch <- t
		} else {
			c.rm.Unlock()
		}
	}(oneTask)
	return nil
}

func (c *Coordinator) Notify(task *Task, rsp *EmptyMsg) error {
	c.rm.Lock()
	defer c.rm.Unlock()
	tid := task.TaskId
	if task.TaskType == 1 { // map
		if c.mtasks[tid].state == RUNNING {
			c.mtasks[tid].state = COMPLETE
			c.mleft--
			if c.mleft == 0 {
				c.rcond.Signal()
			}
		}
	} else { // reduce
		if c.rtasks[tid].state == RUNNING {
			c.rtasks[tid].state = COMPLETE
			c.rleft--
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	c.rm.Lock()
	defer c.rm.Unlock()
	ret = (c.rleft == 0)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		taskch:  make(chan *task),
		mleft:   len(files),
		rleft:   nReduce,
		nreduce: nReduce,
	}
	c.rcond = sync.NewCond(&c.rm)
	for i := 0; i < len(files); i++ {
		oneTask := task{i, PENDING, 1}
		c.mtasks = append(c.mtasks, &oneTask)
	}
	for i := 0; i < nReduce; i++ {
		oneTask := task{i, PENDING, 2}
		c.rtasks = append(c.rtasks, &oneTask)
	}
	go func() {
		for i := 0; i < len(files); i++ {
			c.taskch <- c.mtasks[i]
		}
	}()
	go func() {
		c.rm.Lock()
		for c.mleft != 0 {
			c.rcond.Wait()
		}
		c.rm.Unlock()
		for i := 0; i < nReduce; i++ {
			c.taskch <- c.rtasks[i]
		}
	}()
	c.server()
	return &c
}
