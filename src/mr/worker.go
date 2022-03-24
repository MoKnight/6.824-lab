package mr

import (
	"encoding/json"
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
	workername := ""
	var outputfile []string
	var maptask string
	var reducetask string

	code, status := MR_NONE, MR_GET_TASK
	for code != MR_FINISHED {
		maptask, code, workername, outputfile = GetMapTask(status, workername, "")

		if code == MR_SUCCESS {
			//execute map task
			maptaskcontent, _ := ioutil.ReadFile(maptask)
			mapres := mapf(maptask, string(maptaskcontent))
			sort.Sort(ByKey(mapres))
			nReduce := len(outputfile)
			var tempfilenme []*os.File
			var tempfile *os.File

			//add temp file part
			for _, i := range outputfile {
				temp, err := ioutil.TempFile("./", i)
				tempfilenme = append(tempfilenme, temp)
				if err != nil {
					fmt.Println("文件创建失败")
					return
				}
			}

			//not good too mant open and close
			for _, i := range mapres {
				n := ihash(i.Key) % nReduce
				enc := json.NewEncoder(tempfilenme[n])
				if err := enc.Encode(&i); err != nil {
					tempfile.Close()
					return
				}
			}

			for num, i := range tempfilenme {
				os.Rename(i.Name(), outputfile[num])
				i.Close()
			}

			_, code, _, _ = GetMapTask(MR_MAP_TASK_FINISHED, workername, maptask)
			status = MR_GET_TASK_SECOND
		} else if code == MR_STILL_WAIT { //map phase has not finished
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	if workername == "" {
		status = MR_GET_TASK
	} else {
		status = MR_GET_TASK_SECOND
	}

	rawreduece := []KeyValue{}
	for code != MR_FINISHED {
		reducetask, code, workername, outputfile = GetReduceTask(status, workername, "")
		if code == MR_SUCCESS {
			rawreduece = rawreduece[0:0]
			//execute reduce task
			for _, tempfilename := range outputfile {
				tempfile, _ := os.OpenFile(tempfilename, os.O_CREATE|os.O_RDWR, 0644)
				dec := json.NewDecoder(tempfile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					rawreduece = append(rawreduece, kv)
				}
			}
			sort.Sort(ByKey(rawreduece))

			finalfile, err := ioutil.TempFile("./", reducetask)
			if err != nil {
				fmt.Println("文件创建失败")
				return
			}

			i := 0
			for i < len(rawreduece) {
				j := i + 1
				for j < len(rawreduece) && rawreduece[j].Key == rawreduece[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, rawreduece[k].Value)
				}
				output := reducef(rawreduece[i].Key, values)

				fmt.Fprintf(finalfile, "%v %v\n", rawreduece[i].Key, output)
				i = j
			}
			os.Rename(finalfile.Name(), reducetask)
			finalfile.Close()

			_, code, _, _ = GetReduceTask(MR_REDUCE_TASK_FINISHED, workername, reducetask)
			status = MR_GET_TASK_SECOND
		} else if code == MR_STILL_WAIT { //map phase has not finished
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

func GetMapTask(status int, workername string, str string) (string, int, string, []string) {
	req := GetTaskMes{}
	req.Status = status
	req.Name = workername
	req.File = str
	rep := GetTaskMes{}
	ok := call("Coordinator.GetMapTask", &req, &rep)
	if ok {
		if rep.Status == MR_SUCCESS {
			return rep.File, rep.Status, rep.Name, rep.Outputfile
		} else {
			return "", rep.Status, rep.Name, nil
		}
	} else {
		return "", MR_FINISHED, "", nil
	}
}

func GetReduceTask(status int, workername string, str string) (string, int, string, []string) {
	req := GetTaskMes{}
	req.Status = status
	req.Name = workername
	req.File = str
	rep := GetTaskMes{}
	ok := call("Coordinator.GetReduceTask", &req, &rep)
	if ok {
		if rep.Status == MR_SUCCESS {
			return rep.File, rep.Status, rep.Name, rep.Outputfile
		} else {
			return "", rep.Status, rep.Name, nil
		}
	} else {
		return "", MR_FINISHED, "", nil
	}
}

// func FileExist(path string) bool {
// 	_, err := os.Lstat(path)
// 	return !os.IsNotExist(err)
// }

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
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":2222")
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
