package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//first get map task
	var workername string
	var maptask string
	code, status := MR_NONE, MR_GET_TASK
	for {
		maptask, code, workername = GetMapTask(status)
		if code == MR_SUCCESS {
			status = MR_GET_TASK_SECOND
			//execute map task
			maptaskcontent, _ := ioutil.ReadFile(maptask)
			mapres := mapf(maptask, string(maptaskcontent))
			intermedname := "mr-med-" + workername
			sort.Sort(ByKey(mapres))
			intermedfile, _ := os.Create(intermedname)

			for _, i := range mapres {
				fmt.Fprintf(intermedfile, "%v %v\n", i.Key, i.Value)
			}
			code = CommitMapTask()
		} else if code == MR_STILL_WAIT { //map phase has not finished
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	//assign the worker the map worker has been done and get reduce task
	code := GetReduceTask()
	for code == MR_STILL_WAIT { //map phase has not finished
		time.Sleep(time.Second)
		code = GetReduceTask()
	}
	if code == MR_FINISHED {
		return
	} else if code == MR_SUCCESS {
		//execute reduce task
		reducef()
	}
}

func GetMapTask(status int) (string, int, string) {
	req := GetMapTaskReq{}
	req.status = status
	rep := GetMapTaskRep{}
	ok := call("Coordinator.GetMapTask", &req, &rep)
	if ok {
		if rep.status == MR_SUCCESS {
			return rep.file, rep.status, rep.name
		} else {
			return "", rep.status, rep.name
		}
	} else {
		return "", MR_FINISHED, ""
	}
}

func CommitMapTask() int {
	req := GetMapTaskReq{}
	req.status = MR_MAP_FINISHED
	rep := GetMapTaskRep{}
	ok := call("Coordinator.GetMapTask", &req, &rep)
	if ok {
		if rep.status == MR_SUCCESS {
			return rep.status
		} else {
			return rep.status
		}
	} else {
		return MR_FINISHED
	}
}

func GetReduceTask() int {
	req := GetReduceTaskReq{}
	rep := GetReduceTaskRep{}
	ok := call("Coordinator.GetReduceTask", &req, &rep)
	if ok {
		if rep.status == MR_SUCCESS {

		} else {

		}
	} else {
		return nil
	}
	return MP_FINISHED_OR_BROKEN
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close() //使用完立即关闭，是短连接

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
