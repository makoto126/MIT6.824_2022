package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const Timeout = 10 * time.Second

type TaskStatus int

const (
	Inited TaskStatus = iota
	Started
	Done
)

type Task struct {
	Id        int
	Filename  string
	StartTime time.Time
	Status    TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	MapTasks      []*Task
	ReduceTasks   []*Task
	MapTaskCh     chan *Task
	ReduceTaskCh  chan *Task
	MapDoneCnt    int
	ReduceDoneCnt int
	MapDoneCh     chan int
	ReduceDoneCh  chan int
	MapStartCh    chan int
	ReduceStartCh chan int
	DoneCh        chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Handler(req *Request, reply *Reply) error {
	if req.Report == MapDone {
		c.MapDoneCh <- req.Id
	}
	if req.Report == ReduceDone {
		c.ReduceDoneCh <- req.Id
	}

	task, ok := <-c.MapTaskCh
	if ok {
		c.MapStartCh <- task.Id
		reply.Action = Map
		reply.Id = task.Id
		reply.N = len(c.ReduceTasks)
		reply.Filename = task.Filename
		return nil
	}

	task, ok = <-c.ReduceTaskCh
	if ok {
		c.ReduceStartCh <- task.Id
		reply.Action = Reduce
		reply.Id = task.Id
		reply.N = len(c.MapTasks)
		return nil
	}

	reply.Action = Exit

	return nil
}

func (c *Coordinator) backGround() {
	tick := time.NewTicker(Timeout)
	go func() {
	outer:
		for {
			select {
			case id := <-c.MapStartCh:
				log.Println("map start", id)
				c.MapTasks[id].Status = Started
				c.MapTasks[id].StartTime = time.Now()
			case id := <-c.ReduceStartCh:
				log.Println("reduce start", id)
				c.ReduceTasks[id].Status = Started
				c.ReduceTasks[id].StartTime = time.Now()
			case id := <-c.MapDoneCh:
				if c.MapTasks[id].Status == Started {
					c.MapTasks[id].Status = Done
					c.MapDoneCnt++
					if c.MapDoneCnt == len(c.MapTasks) {
						close(c.MapTaskCh)
					}
				}
			case id := <-c.ReduceDoneCh:
				if c.ReduceTasks[id].Status == Started {
					c.ReduceTasks[id].Status = Done
					c.ReduceDoneCnt++
					if c.ReduceDoneCnt == len(c.ReduceTasks) {
						close(c.ReduceTaskCh)
					}
				}
			case <-tick.C:
				if c.ReduceDoneCnt == len(c.ReduceTasks) {
					close(c.DoneCh)
					break outer
				}
				if c.MapDoneCnt < len(c.MapTasks) {
					for _, task := range c.MapTasks {
						if task.Status == Started && task.StartTime.Add(Timeout).Before(time.Now()) {
							task.Status = Inited
							c.MapTaskCh <- task
						}
					}
				} else {
					for _, task := range c.ReduceTasks {
						if task.Status == Started && task.StartTime.Add(Timeout).Before(time.Now()) {
							task.Status = Inited
							c.ReduceTaskCh <- task
						}
					}
				}
			}
		}
	}()
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
	<-c.DoneCh

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:      make([]*Task, len(files)),
		ReduceTasks:   make([]*Task, nReduce),
		MapTaskCh:     make(chan *Task, len(files)),
		ReduceTaskCh:  make(chan *Task, nReduce),
		MapStartCh:    make(chan int),
		ReduceStartCh: make(chan int),
		MapDoneCh:     make(chan int),
		ReduceDoneCh:  make(chan int),
		DoneCh:        make(chan struct{}),
	}
	for i, file := range files {
		c.MapTasks[i] = &Task{
			Id:       i,
			Filename: file,
			Status:   Inited,
		}
		c.MapTaskCh <- c.MapTasks[i]
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &Task{
			Id:     i,
			Status: Inited,
		}
		c.ReduceTaskCh <- c.ReduceTasks[i]
	}

	c.backGround()
	c.server()
	return &c
}
