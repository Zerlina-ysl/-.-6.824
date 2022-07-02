package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"
//定义rpc通信中的响应实体
type Task struct{
	TaskType TaskType   //----map/reduce
	TaskId int //----唯一标识Task
	//Filename string //-----输入文件 对文件内容进行操作
	FileSlice []string //------输入文件的切片，map一个文件对应一个文件，reduce对应多个文件
	ReducerNum int //----传入的reducer的数量 用于hash
}

//---定义请求实体 但是rpc的方法只是获取一个Task 因此可以什么都不传 为空
type TaskArgs struct {}


//对于任务的类型
type TaskType int

//对于分配任务阶段
type Phase int

//对于任务所处状态
type State int


//---任务的类型
const(
	mapTask TaskType=iota
	reduceTask
	waitingTask //----任务分发完，但是任务还没完成，等待任务结束
	exitTask		//----任务分发完成,任务被终止
)
//-----任务所处阶段
const(
	mapPhase Phase=iota
	reducePhase
	allDone  //-----所有任务分发完成
)
//-----任务状态
const(
	working State=iota
	waiting
	done
)


//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 定义结构体  发送的请求 和返回的响应
//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
//多线程下获取唯一id
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
