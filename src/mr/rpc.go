/*
 * @Description:
 * @version:
 * @Author: MoonKnight
 * @Date: 2022-03-10 15:48:39
 * @LastEditors: MoonKnight
 * @LastEditTime: 2022-03-22 21:21:15
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

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
const (
	MR_NONE                 = iota
	MR_SUCCESS              //succeed to get task
	MR_MAP_TIME_OUT         //execute timeout
	MR_REDUCE_TIME_OUT      //execute timeout
	MR_STILL_WAIT           //wait for map phase end
	MR_GET_TASK             //first time to het task,have no identified name
	MR_GET_TASK_SECOND      //second time to get task, already have a name
	MR_MAP_TASK_FINISHED    //commit map task
	MR_REDUCE_TASK_FINISHED //commit reduce task
	MR_FINISHED             //all map and reduce task have finished

	//map_or_reduce_status
	MR_MAP_NOT_START  //to note task status,target task never start
	MR_MAP_IN_PROCESS //to note task status.tarhet task is in process
	MR_MAP_FINISHED   //to note task status,target task is finished
	MR_REDUCE_NOT_START
	MR_REDUCE_IN_PROCESS //to note task status.tarhet task is in process
	MR_REDUCE_FINISHED   //to note task status,target task is finished
)

type GetTaskMes struct {
	Status     int
	File       string
	Name       string
	Outputfile []string
}

// type GetTaskRep struct {
// 	Status     int
// 	Name       string
// 	File       string
// 	Outputfile []string
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
