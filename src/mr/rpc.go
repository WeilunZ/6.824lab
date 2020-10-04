package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

type TaskState int

const (
	TaskMap    TaskState = 1
	TaskReduce TaskState = 2
	TaskWait   TaskState = 3
	TaskEnd    TaskState = 4
)

type TaskInfoInterface interface {
	OutOfTime() bool
	SetNow()
	GetRIndex() int
	GetMIndex() int
	GetNReduce() int
	GetNFiles() int
	GetFileName() string
}

type TaskInfo struct {
	State     TaskState
	BeginTime time.Time
	Filename  string
	RIndex    int
	MIndex    int
	NReduce   int
	NFiles    int //for reduce
}

func (ts *TaskInfo) SetNow() {
	ts.BeginTime = time.Now()
}

func (ts *TaskInfo) GetRIndex() int {
	return ts.RIndex
}

func (ts *TaskInfo) GetMIndex() int {
	return ts.MIndex
}

func (ts *TaskInfo) GetNReduce() int {
	return ts.NReduce
}

func (ts *TaskInfo) GetNFiles() int {
	return ts.NFiles
}

func (ts *TaskInfo) GetFileName() string {
	return ts.Filename
}

func (ts *TaskInfo) OutOfTime() bool {
	return ts.BeginTime.Add(time.Second * 10).Before(time.Now())
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskTaskArgs struct {
}

type AskTaskReply struct {
	*TaskInfo
}

type TaskDoneArgs struct {
	*TaskInfo
}

type TaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
