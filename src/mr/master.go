package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IntermediatePattern = "mr-%d-%d"
	OutputPattern       = "mr-out-%d"
)

type TaskQueue struct {
	taskArray []TaskInfoInterface
	mu        sync.RWMutex
}

func (q *TaskQueue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.taskArray)
}

func (q *TaskQueue) Push(task TaskInfoInterface) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if task == nil {
		return
	}
	q.taskArray = append(q.taskArray, task)
}

func (q *TaskQueue) Top() TaskInfoInterface {
	if len(q.taskArray) > 0 {
		return q.taskArray[0]
	}
	return nil
}

func (q *TaskQueue) Pop() TaskInfoInterface {
	if q.Size() == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	top := q.taskArray[0]
	q.taskArray = q.taskArray[1:]
	return top
}

func (q *TaskQueue) RemoveTask(mIndex, rIndex *int) {
	q.mu.Lock()
	for i, task := range q.taskArray {
		match := false
		if mIndex != nil && rIndex != nil {
			if task.GetMIndex() == *mIndex && task.GetRIndex() == *rIndex {
				match = true
			}
		} else if mIndex != nil {
			if task.GetMIndex() == *mIndex {
				match = true
			}
		} else if rIndex != nil {
			if task.GetRIndex() == *rIndex {
				match = true
			}
		}
		if match {
			q.taskArray = append(q.taskArray[:i], q.taskArray[i+1:]...)
			break
		}
	}
	q.mu.Unlock()
}

type Master struct {
	// Your definitions here.
	isDone             bool
	inputFiles         []string
	nReduce            int
	mapTaskWaitingQ    *TaskQueue
	mapTaskRunningQ    *TaskQueue
	reduceTaskWaitingQ *TaskQueue
	reduceTaskRunningQ *TaskQueue
}

func (m *Master) init(files []string, nReduce int) {
	m.inputFiles = files
	m.nReduce = nReduce
	m.mapTaskWaitingQ = newTaskQueue()
	m.mapTaskRunningQ = newTaskQueue()
	m.reduceTaskWaitingQ = newTaskQueue()
	m.reduceTaskRunningQ = newTaskQueue()

	for i, filename := range m.inputFiles {
		task := &TaskInfo{
			State:     TaskMap,
			BeginTime: time.Now(),
			Filename:  filename,
			MIndex:    i,
			NReduce:   nReduce,
		}
		m.mapTaskWaitingQ.Push(task)
	}
}

func (m *Master) schedule() {
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-tick.C:
			m.scheduleOnceTimeoutTask()
		}
	}
}

func (m *Master) scheduleOnceTimeoutTask() {
	m.mapTaskRunningQ.mu.Lock()
	for {
		mapTask := m.mapTaskRunningQ.Top()
		if mapTask != nil && mapTask.OutOfTime() {
			m.mapTaskRunningQ.Pop()
			mapTask.SetNow()
			m.mapTaskWaitingQ.Push(mapTask)
		} else {
			break
		}
	}
	m.mapTaskRunningQ.mu.Unlock()

	m.reduceTaskRunningQ.mu.Lock()
	for {
		reduceTask := m.reduceTaskRunningQ.Top()
		if reduceTask != nil && reduceTask.OutOfTime() {
			m.reduceTaskRunningQ.Pop()
			reduceTask.SetNow()
			m.reduceTaskWaitingQ.Push(reduceTask)
		} else {
			break
		}
	}
	m.reduceTaskRunningQ.mu.Unlock()
}

func (m *Master) scheduleOnceReduce() {
	for rIndex := 0; rIndex < m.nReduce; rIndex++ {
		task := &TaskInfo{
			State:     TaskReduce,
			BeginTime: time.Now(),
			RIndex:    rIndex,
			NReduce:   m.nReduce,
			NFiles:    len(m.inputFiles),
		}
		m.reduceTaskWaitingQ.Push(task)
	}
}

func newTaskQueue() *TaskQueue {
	return &TaskQueue{
		taskArray: make([]TaskInfoInterface, 0),
		mu:        sync.RWMutex{},
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	if reduceTask := m.reduceTaskWaitingQ.Pop(); reduceTask != nil {
		reduceTask.SetNow()
		m.reduceTaskRunningQ.Push(reduceTask)
		reply.TaskInfo = reduceTask.(*TaskInfo)
		return nil
	}
	if mapTask := m.mapTaskWaitingQ.Pop(); mapTask != nil {
		mapTask.SetNow()
		m.mapTaskRunningQ.Push(mapTask)
		reply.TaskInfo = mapTask.(*TaskInfo)
		return nil
	}
	if m.mapTaskRunningQ.Size() > 0 && m.reduceTaskRunningQ.Size() > 0 {
		reply.TaskInfo = &TaskInfo{State: TaskWait}
		return nil
	}
	reply.TaskInfo = &TaskInfo{State: TaskEnd}
	m.isDone = true
	return nil
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	switch args.TaskInfo.State {
	case TaskMap:
		m.mapTaskRunningQ.RemoveTask(&(args.MIndex), nil)
		if m.mapTaskWaitingQ.Size() == 0 && m.mapTaskRunningQ.Size() == 0 {
			m.scheduleOnceReduce()
		}
	case TaskReduce:
		m.reduceTaskRunningQ.RemoveTask(nil, &(args.RIndex))
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.isDone

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.init(files, nReduce)
	go m.schedule()

	m.server()
	return &m
}
