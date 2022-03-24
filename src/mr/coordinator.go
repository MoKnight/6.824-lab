/*
 * @Description:
 * @version:
 * @Author: MoonKnight
 * @Date: 2022-03-10 15:48:39
 * @LastEditors: MoonKnight
 * @LastEditTime: 2022-03-23 20:54:00
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
	workers            int      //wording workers
	current_worker     int      //workers needed
	done_map_worker    int      //the number workers in map has finish their job
	done_reduce_worker int      //the number workers in reduce has finish their job
	rawfiles           []string //files needs to be resolved
	dividedfiles       []string //divde
	//medfiles           []string
	finalfiles        []string
	m                 sync.Map //contain map task status
	r                 sync.Map //contain reduce task status
	fixed_map_time    time.Duration
	fixed_reduce_time time.Duration
	lock              sync.Mutex
}

type taskstatus struct {
	filename  []string  //output file name
	ddl       time.Time //time to  finsih the task
	finished  int       //whether finished
	worker    string    //current working worker
	blacklist []string  //workers has tried but failed
}

// Your code here -- RPC handlers for the worker to call.
//
// send map task to worker
//
func (c *Coordinator) GetMapTask(request *GetTaskMes, reply *GetTaskMes) error {
	//this phase has already ended
	c.lock.Lock()
	if c.done_map_worker == len(c.rawfiles) {
		reply.Status = MR_MAP_FINISHED
		c.lock.Unlock()
		return nil
	}
	c.lock.Unlock()

	//fisrt time to get map_task
	if request.Status == MR_GET_TASK {
		c.lock.Lock()
		worker := fmt.Sprint(c.current_worker)
		//worker := "worker" + fmt.Sprint(c.current_worker)
		c.current_worker++
		for _, str := range c.rawfiles {
			if c.JudgeFitJob_Map(str, worker, reply) {
				c.lock.Unlock()
				return nil
			}
		}
		c.lock.Unlock()
		reply.Status = MR_STILL_WAIT
		//commit or second get
	} else if request.Status == MR_GET_TASK_SECOND {
		c.lock.Lock()
		for _, str := range c.rawfiles {
			if c.JudgeFitJob_Map(str, request.Name, reply) {
				c.lock.Unlock()
				return nil
			}
		}
		c.lock.Unlock()
		reply.Status = MR_STILL_WAIT
	} else if request.Status == MR_MAP_TASK_FINISHED {
		if temp, ok := c.m.Load(request.File); ok {
			temp_status := temp.(taskstatus)
			if temp_status.worker == request.Name {
				if !temp_status.ddl.Before(time.Now()) {
					temp_status.finished = MR_MAP_FINISHED
					c.m.Store(request.File, temp_status)

					//need to lock, to avoid race
					c.lock.Lock()
					c.done_map_worker++
					if c.done_map_worker == len(c.rawfiles) {
						reply.Status = MR_MAP_FINISHED
					} else {
						reply.Status = MR_STILL_WAIT
					}
					c.lock.Unlock()
				} else {
					temp_status.blacklist = append(temp_status.blacklist, request.Name)
					temp_status.finished = MR_MAP_NOT_START
					c.m.Store(request.File, temp_status)
					c.lock.Lock()
					for _, str := range c.rawfiles {
						if c.JudgeFitJob_Map(str, request.Name, reply) {
							c.lock.Unlock()
							return nil
						}
					}
					c.lock.Unlock()
					reply.Status = MR_STILL_WAIT
				}
			} else {
				c.lock.Lock()
				for _, str := range c.rawfiles {
					if c.JudgeFitJob_Map(str, request.Name, reply) {
						c.lock.Unlock()
						return nil
					}
				}
				c.lock.Unlock()
				reply.Status = MR_STILL_WAIT
			}
		}
	}
	return nil
}

//
// send recude task to worker
//
func (c *Coordinator) GetReduceTask(request *GetTaskMes, reply *GetTaskMes) error {
	//this phase has already ended
	c.lock.Lock()
	if c.done_reduce_worker == c.workers {
		reply.Status = MR_REDUCE_FINISHED
		c.lock.Unlock()
		return nil
	}
	c.lock.Unlock()

	//fisrt time to get reduce_task
	if request.Status == MR_GET_TASK {
		c.lock.Lock()
		worker := fmt.Sprint(c.current_worker)
		//worker := "worker" + fmt.Sprint(c.current_worker)
		c.current_worker++
		for _, str := range c.finalfiles {
			if c.JudgeFitJob_Reduce(str, worker, reply) {
				c.lock.Unlock()
				return nil
			}
		}
		c.lock.Unlock()
		reply.Status = MR_STILL_WAIT
		//commit or second get
	} else if request.Status == MR_GET_TASK_SECOND {
		c.lock.Lock()
		for _, str := range c.finalfiles {
			if c.JudgeFitJob_Reduce(str, request.Name, reply) {
				c.lock.Unlock()
				return nil
			}
		}
		c.lock.Unlock()
		reply.Status = MR_STILL_WAIT
	} else if request.Status == MR_REDUCE_TASK_FINISHED {
		if temp, ok := c.r.Load(request.File); ok {
			temp_status := temp.(taskstatus)
			if temp_status.worker == request.Name {
				//in time succeed
				if !temp_status.ddl.Before(time.Now()) {
					temp_status.finished = MR_REDUCE_FINISHED
					c.r.Store(request.File, temp_status)

					//need to lock, to avoid race
					c.lock.Lock()
					c.done_reduce_worker++
					if c.done_reduce_worker == c.workers {
						reply.Status = MR_REDUCE_FINISHED
					} else {
						reply.Status = MR_STILL_WAIT
					}
					c.lock.Unlock()
					//out of time
				} else {
					temp_status.blacklist = append(temp_status.blacklist, request.Name)
					temp_status.finished = MR_REDUCE_NOT_START
					c.r.Store(request.File, temp_status)
					c.lock.Lock()
					for _, str := range c.finalfiles {
						if c.JudgeFitJob_Reduce(str, request.Name, reply) {
							c.lock.Unlock()
							return nil
						}
					}
					c.lock.Unlock()
					reply.Status = MR_STILL_WAIT
				}
				//already time out, select again
			} else {
				c.lock.Lock()
				for _, str := range c.finalfiles {
					if c.JudgeFitJob_Reduce(str, request.Name, reply) {
						c.lock.Unlock()
						return nil
					}
				}
				c.lock.Unlock()
				reply.Status = MR_STILL_WAIT
			}
		}
	}
	return nil
}

func (c *Coordinator) JudgeFitJob_Map(str string, worker string, reply *GetTaskMes) bool {
	if temp, ok := c.m.Load(str); ok {
		temp_status := temp.(taskstatus)
		if temp_status.finished == MR_MAP_FINISHED {
			return false
		} else if temp_status.finished == MR_MAP_IN_PROCESS {
			if temp_status.ddl.Before(time.Now()) && temp_status.Judgeblacklist(worker) {
				temp_status.blacklist = append(temp_status.blacklist, temp_status.worker)
			} else {
				return false
			}
		} else {
			temp_status.finished = MR_MAP_IN_PROCESS
		}
		temp_status.worker = worker
		temp_status.ddl = time.Now().Add(c.fixed_map_time)
		c.m.Store(str, temp_status)

		reply.File = str
		reply.Name = worker
		reply.Status = MR_SUCCESS
		reply.Outputfile = temp_status.filename
		return true
	}
	return false
}

func (c *Coordinator) JudgeFitJob_Reduce(str string, worker string, reply *GetTaskMes) bool {
	if temp, ok := c.r.Load(str); ok {
		temp_status := temp.(taskstatus)
		if temp_status.finished == MR_REDUCE_FINISHED {
			return false
		} else if temp_status.finished == MR_REDUCE_IN_PROCESS {
			if temp_status.ddl.Before(time.Now()) && temp_status.Judgeblacklist(worker) {
				temp_status.blacklist = append(temp_status.blacklist, temp_status.worker)
			} else {
				return false
			}
		} else {
			temp_status.finished = MR_REDUCE_IN_PROCESS
		}
		temp_status.worker = worker
		temp_status.ddl = time.Now().Add(c.fixed_reduce_time)
		c.r.Store(str, temp_status)

		reply.File = str
		reply.Name = worker
		reply.Status = MR_SUCCESS
		reply.Outputfile = temp_status.filename
		return true
	}
	return false
}

func (t *taskstatus) Judgeblacklist(worker string) bool {
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
	//l, e := net.Listen("tcp", ":2222")
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
	//fmt.Println(c.done_map_worker)
	//fmt.Println(c.workers) c.done_map_worker == len(c.rawfiles) &&
	c.lock.Lock()
	if c.done_reduce_worker == c.workers {
		ret = true
	}
	c.lock.Unlock()
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
		fileName := prefix_map_task_name + fmt.Sprint(i)
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
			for _, str := range content_str[(i)*(len(content_str)/nReduce):] {
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
	c.done_reduce_worker = 0
	c.current_worker = 0
	c.rawfiles = files
	//c.dividedfiles = divdefils(files, nReduce) //divde the input file into same size file
	//c.medfiles = []string{}
	c.finalfiles = []string{}
	c.fixed_map_time, _ = time.ParseDuration("10s")
	c.fixed_reduce_time, _ = time.ParseDuration("10s")

	for num, str := range c.rawfiles {
		temp := taskstatus{}
		temp.finished = MR_MAP_NOT_START
		temp.blacklist = []string{}
		for i := 0; i < nReduce; i++ {
			medname := "mr-" + fmt.Sprint(num) + "-" + fmt.Sprint(i)
			temp.filename = append(temp.filename, medname)
		}
		c.m.Store(str, temp)
	}

	for i := 0; i < nReduce; i++ {
		finalname := "mr-out-" + fmt.Sprint(i)
		c.finalfiles = append(c.finalfiles, finalname)

		temp := taskstatus{}
		temp.finished = MR_REDUCE_NOT_START
		temp.blacklist = []string{}
		for num, _ := range c.rawfiles {
			medname := "mr-" + fmt.Sprint(num) + "-" + fmt.Sprint(i)
			temp.filename = append(temp.filename, medname)
		}
		c.r.Store(finalname, temp)
	}

	c.server()
	return &c
}
