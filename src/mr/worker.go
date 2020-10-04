package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	for {
		task, ok := callAskTask()
		if !ok {
			break
		}

		log.Printf("callAskTask response : %#v\n", task.TaskInfo)

		switch task.State {
		case TaskMap:
			runMap(task.TaskInfo, mapf)
		case TaskReduce:
			runReduce(task.TaskInfo, reducef)
		case TaskWait:
			time.Sleep(time.Millisecond * 100)
		case TaskEnd:
			log.Println("Worker finishes.")
			return
		}
	}

}

func runMap(task *TaskInfo, mapf func(string, string) []KeyValue) {
	content := readFile(task.Filename)
	intermediate := make([]KeyValue, 0)
	kva := mapf(task.Filename, string(content))
	intermediate = append(intermediate, kva...)

	outFiles := make([]*os.File, task.NReduce)
	fileEncoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		outFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-intermediate-*")
		fileEncoders[i] = json.NewEncoder(outFiles[i])
	}

	for _, kv := range intermediate {
		outIndex := ihash(kv.Key) % task.NReduce
		err := fileEncoders[outIndex].Encode(kv)
		if err != nil {
			log.Fatalf("runMap json encode failed")
		}
	}

	for index, file := range outFiles {
		outName := fmt.Sprintf(IntermediatePattern, task.MIndex, index)
		path := filepath.Join(file.Name())
		os.Rename(path, outName)
		file.Close()
	}

	callTaskDone(task)
}

func runReduce(task *TaskInfo, reducef func(string, []string) string) {

	kva := make([]KeyValue, 0)
	for i := 0; i < task.NFiles; i++ {
		filename := fmt.Sprintf(IntermediatePattern, i, task.RIndex)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("runReduce open file fatal error: %v", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	f, err := ioutil.TempFile("mr-tmp", "mr-out-*")
	if err != nil {
		log.Fatalf("open tmp file error crash: %v", err)
	}
	defer f.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)

		i = j
	}

	oldPath := filepath.Join(f.Name())
	os.Rename(oldPath, fmt.Sprintf(OutputPattern, task.RIndex))
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return content
}

// callAskTask ask for a map/reduce/other type task from master.
func callAskTask() (*AskTaskReply, bool) {
	args := &AskTaskArgs{}

	reply := AskTaskReply{}
	ok := call("Master.AskTask", &args, &reply)

	return &reply, ok
}

// callTaskDone notifies the master that the map task has finished.
func callTaskDone(task *TaskInfo) {
	args := &TaskDoneArgs{task}
	reply := TaskDoneReply{}
	call("Master.TaskDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
