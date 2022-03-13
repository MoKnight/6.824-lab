/*
 * @Description:
 * @version:
 * @Author: MoonKnight
 * @Date: 2022-03-10 15:48:39
 * @LastEditors: MoonKnight
 * @LastEditTime: 2022-03-12 20:46:53
 */
package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"time"
)

type Coordinator struct {
	// Your definitions here.
	workers           int      //wording workers
	current_worker    int      //workers needed
	done_map_worker   int      //the number workers has finish their job
	rawfiles          []string //files needs to be resolved
	dividedfiles      []string //divde
	medfiles          []string
	m                 sync.Map
	fixed_map_time    time.Duration
	fixed_reduce_time time.Duration
}

type maptaskstatus struct {
	ddl       time.Time //time to  finsih the task
	finished  int       //whether finished
	worker    string
	blacklist []string
}

// Your code here -- RPC handlers for the worker to call.
//
// send map task to worker
//
func (c *Coordinator) GetMapTask(request *GetMapTaskReq, reply *GetMapTaskRep) error {

	if c.done_map_worker == c.workers {
		reply.status = MR_MAP_FINISHED
		return nil
	}

	//fisrt time to get map_task
	if request.status == MR_GET_TASK {
		worker := "worker" + string(c.current_worker)
		c.current_worker++
		for _, str := range c.dividedfiles {
			if c.JudgeFitJob(str, worker, *reply) {
				break
			}
		}
		//commit or second get
	} else if request.status == MR_GET_TASK_SECOND {
		for _, str := range c.dividedfiles {
			if c.JudgeFitJob(str, request.name, *reply) {
				break
			}
		}
	} else if request.status == MR_MAP_TASK_FINISH {
		if temp, ok := c.m.Load(request.file); ok {
			temp_status := temp.(maptaskstatus)
			if temp_status.worker == request.name {
				if !temp_status.ddl.Before(time.Now()) {
					temp_status.finished = MR_MAP_FINISHED
					c.done_map_worker++
					c.medfiles = append(c.medfiles, request.medfile)
					if c.done_map_worker == c.workers {
						reply.status = MR_MAP_FINISHED
					} else {
						reply.status = MR_STILL_WAIT
					}
				} else {
					temp_status.blacklist = append(temp_status.blacklist, request.name)
					temp_status.finished = MR_MAP_NOT_START
					for _, str := range c.dividedfiles {
						if c.JudgeFitJob(str, request.name, *reply) {
							break
						}
					}
				}
			} else {
				for _, str := range c.dividedfiles {
					if c.JudgeFitJob(str, request.name, *reply) {
						break
					}
				}
			}
		}
	}
	return nil
}

//
// send recude task to worker
//
func (c *Coordinator) GetReduceTask(request *GetReduceTaskReq, reply *GetReduceTaskRep) error {

	return nil
}

func (c *Coordinator) JudgeFitJob(str string, worker string, reply GetMapTaskRep) bool {
	if temp, ok := c.m.Load(str); ok {
		temp_status := temp.(maptaskstatus)
		if temp_status.finished == MR_MAP_FINISHED {
			return false
		} else if temp_status.finished == MR_MAP_IN_PROCESS {
			if temp_status.ddl.Before(time.Now()) && temp_status.Judgeblacklist(worker) {
				temp_status.blacklist = append(temp_status.blacklist, temp_status.worker)
				temp_status.worker = worker
				temp_status.ddl = time.Now().Add(c.fixed_map_time)

				reply.name = temp_status.worker
				return true
			}
		} else {
			temp_status.finished = MR_MAP_IN_PROCESS
			c.m.Store(str, temp_status)
			reply.file = str
			reply.status = MR_SUCCESS
			return true
		}
	}
	return false
}

func (t *maptaskstatus) Judgeblacklist(worker string) bool {
	for _, str := range t.blacklist {
		if str == worker {
			return false
		}
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	ret := false

	// Your code here.
	//send info to find whether the workers has finished
	if c.done_map_worker == c.workers {
		ret = true
	}

	return ret
}

func divdefils(files []string, nReduce int) []string {
	var content []byte
	var res []string
	for _, i := range files {
		tempcontent, _ := ioutil.ReadFile(i)
		content = append(content, tempcontent...)
	}
	content_str := strings.Split(string(content), "\n")

	prefix_map_task_name := "maptask-"
	for i := 0; i < nReduce; i++ {
		fileName := prefix_map_task_name + string(i)
		res = append(res, fileName)
		dstFile, err := os.Create(fileName)
		if err != nil {
			fmt.Println(err.Error())
			return nil
		}
		defer dstFile.Close()

		if i != nReduce-1 {
			for _, str := range content_str[i*(len(content_str)/nReduce) : i*(len(content_str)/nReduce)] {
				dstFile.WriteString(str + "\n")
			}
		} else {
			for _, str := range content_str[(i-1)*(len(content_str)/nReduce):] {
				dstFile.WriteString(str + "\n")
			}
		}
	}
	return res
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.workers = nReduce
	c.done_map_worker = 0
	c.current_worker = 0
	c.rawfiles = files
	c.dividedfiles = divdefils(files, nReduce) //divde the input file into same size file
	c.fixed_map_time, _ = time.ParseDuration("10s")
	c.fixed_reduce_time, _ = time.ParseDuration("10s")

	for _, str := range c.dividedfiles {
		temp := maptaskstatus{}
		temp.finished = MR_MAP_NOT_START
		temp.blacklist = []string{}
		c.m.Store(str, temp)
	}

	c.server()
	return &c
}
