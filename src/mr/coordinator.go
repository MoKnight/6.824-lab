/*
 * @Description:
 * @version:
 * @Author: MoonKnight
 * @Date: 2022-03-10 15:48:39
 * @LastEditors: MoonKnight
 * @LastEditTime: 2022-03-11 00:14:36
 */
package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	workers      int      //wording workers
	doneworkers  int      //the number workers has finish their job
	rawfiles     []string //files needs to be resolved
	dividedfiles []string //divde
	m            sync.Map
}

// Your code here -- RPC handlers for the worker to call.
//
// send map task to worker
//
func (c *Coordinator) GetMapTask(ctx context.Context, request *string, reply *string) error {

	c.m.Load()
	http.RemoteAddr()
	return nil
}

//
// send recude task to worker
//
func (c *Coordinator) GetReduceTask(request *string, reply *string) error {

	return nil
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
	if c.doneworkers == c.workers {
		ret = true
	}

	return ret
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
	c.doneworkers = 0
	c.rawfiles = files

	c.server()
	return &c
}
