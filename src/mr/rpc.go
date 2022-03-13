/*
 * @Description:
 * @version:
 * @Author: MoonKnight
 * @Date: 2022-03-10 15:48:39
 * @LastEditors: MoonKnight
 * @LastEditTime: 2022-03-12 20:47:12
 */
package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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
const (
	MR_NONE = iota
	MR_SUCCESS
	MR_MAP_TIME_OUT
	MR_REDUCE_TIME_OUT
	MR_STILL_WAIT
	MR_GET_TASK
	MR_GET_TASK_SECOND
	MR_MAP_TASK_FINISH
	MR_FINISHED

	//map_or_reduce_status
	MR_MAP_NOT_START
	MR_MAP_IN_PROCESS
	MR_MAP_FINISHED
)

type GetMapTaskReq struct {
	status  int
	file    string
	name    string
	medfile string
}

type GetMapTaskRep struct {
	status int
	name   string
	file   string
}

type GetReduceTaskReq struct {
	status  int
	outfile string
	name    string
}

type GetReduceTaskRep struct {
	status int
	name   string
	file   string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
