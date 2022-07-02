package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//coordinator，作为一个RPC server，会并发；不要忘记lock共享数据。
var mu sync.Mutex

//---主节点维持多种数据结构。它会存储每个map和reduce任务的状态（空闲、处理中、完成），和每台工作机器的ID（对应非空闲的任务）
type Coordinator struct {
	// Your definitions here.
	ReducerNum        int      // reducer任务的个数
	TaskId            int      // 每台工作机器的ID（对应非空闲的任务）
	Phase             Phase    // 存储每个map和reduce任务的状态
	files             []string //传入的文件数组
	taskMetaHolder    TaskMetaHolder
	TaskChannelReduce chan *Task //长度是nreduce的个数，与reduce的任务个数有关
	TaskChannelMap    chan *Task //长度是len(files) 与输入文件的个数有关Ò

}

//----- 存放全部元信息的map
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo //使用map 通过下标hash快速定位
}

// ------保存任务的元数据：状态以及状态的更改
type TaskMetaInfo struct {
	state     State //任务的状态
	TaskAdr   *Task //传入任务的指针。指向任务，当任务从通道取出，还能通过地址标记这个任务已经完成
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

//---------- ------rpc方法：MarkFinished
func (coordinator *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	//switch 默认情况下 case 最后自带 break 语句，匹配成功后就不会执行其他 case
	case mapTask:
		meta, ok := coordinator.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == working {
			meta.state = done
			//fmt.Println("Map task id [%d] 's map is finished.\n", args.TaskId)
		} else {
			fmt.Println("Map task id [%d]   is finished already.\n", args.TaskId)
		}
		break
	case reduceTask:
		{
		meta,ok:=coordinator.taskMetaHolder.MetaMap[args.TaskId]
		if ok&& meta.state==working{
			meta.state=done
		} else{
			fmt.Println("reduce task id[%d] is finished,already!!!\n",args.TaskId)
		}
		break

		}

	default:
		panic("the task type undifined!!!")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
// tpc通信过程
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
// 当map阶段的map任务全部完成，那么该方法需要返回true，使协调者可以exit程序
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == allDone {
		fmt.Println("all tasks are finished,the coordinator will be exit")
		return true
	} else {
		return false
	}
}

//-----------------coordinator的初始化
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//初始化coordinator
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		Phase:             mapPhase, //初始化时所处Ò状态为map任务
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			//任务的总数是files+Reducer的数量 不是相乘关系
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	//初始化任务，将map任务放到map管道，将taskMetaInfo放到taskMetaHolder
	c.makeMapTasks(files)
	// Your code here.
	c.server()
	//undo


	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	//对于每一个输入文件都要进行任务的初始化
	for _, file := range files {
		//定义每个file对应的Task
		id := c.generateTaskId()
		task := Task{
			TaskId:     id,
			TaskType:   mapTask, //初始化时 进行map任务
			ReducerNum: c.ReducerNum,
			//将遍历到的每一个file作为一个元素传入数组
			FileSlice:   []string{file},
		}
		taskMetaInfo := TaskMetaInfo{
			state:   waiting, //任务等待被执行
			TaskAdr: &task,   //指针指向当前任务
		}
		//MetaMap map[int]*TaskMetaInfo 将taskMetaInfo放到taskMetaHolder
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("make a map task:", &task)
		//make(chan *Task,len(files)) 将map任务放到map管道
		//箭头的指向是数据的流向
		c.TaskChannelMap <- &task
	}
}

func (coordinator *Coordinator) makeReduceTasks() {
	for i := 0; i < coordinator.ReducerNum; i++ {
		id := coordinator.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  reduceTask,
			//挑选执行reduce方法的结点
			FileSlice: selectReduceName(i),
		}
		taskMetaInfo := TaskMetaInfo{
			state:   waiting, //任务等待被执行
			TaskAdr: &task,
		}
		coordinator.taskMetaHolder.acceptMeta(&taskMetaInfo)
		coordinator.TaskChannelReduce <- &task
	}

}
func selectReduceName(reduceNum int) []string {
	var s []string
	//返回当前目录的根路径名
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		//遍历根目录文件下的文件 将 mr-tmp-------reduceNum的文件作为reduce函数的参数
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

//---通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {
	taskId := c.TaskId
	c.TaskId++
	return taskId
}

//----将taskMetaInfo加入taskMetaHolder的map切片中
func (taskMetaHolder *TaskMetaHolder) acceptMeta(taskMetaInfo *TaskMetaInfo) bool {
	taskId := taskMetaInfo.TaskAdr.TaskId
	//如果当前任务对应的元信息已存在，添加失败
	meta, _ := taskMetaHolder.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		taskMetaHolder.MetaMap[taskId] = taskMetaInfo

	}
	return true
}

//-------------------rpc方法 PollTask
//---- GetTask(): ok := call("Coordinator.PollTask", &args, &reply)
//核心方法：分配任务
func (coordinator *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	//分布式任务，需要上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	//使用defer调用Unlock 临界区会隐式的延伸到函数作用域的最后，"在函数返回前或错误发生前调用Unlock"
	defer mu.Unlock()
	switch coordinator.Phase {
	case mapPhase:
		{
			//在coordinator的任务初始化中 会把等待状态的任务放在channel中 因此可以直接取出
			if len(coordinator.TaskChannelMap) > 0 {
				*reply = *<-coordinator.TaskChannelMap
				if !coordinator.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Println("taskId [%d] is running \n", reply.TaskId)
				}

			} else {
				//chan中的map任务发完后但是又没完成任务
				//此时任务变为等待类型
				reply.TaskType = waitingTask
				if coordinator.taskMetaHolder.checkTaskDone() {
					//checkTaskDone()中分为三阶段讨论返回true:map任务全部完成|reduce任务全部完成|全部任务全部完成
					coordinator.toNextPhase()
				}
				return nil
			}
		}
	case reducePhase:
		{
			if len(coordinator.TaskChannelReduce) > 0 {
				*reply = *<-coordinator.TaskChannelReduce
				if !coordinator.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Println("Reduce-taskid[%d] is running \n", reply.TaskId)
				}
			} else {
				reply.TaskType = waitingTask
				if coordinator.taskMetaHolder.checkTaskDone() {
					coordinator.toNextPhase()

				}
				return nil
			}
		}
	case allDone:
		{
			reply.TaskType = exitTask
		}
	default:
		{
			panic("the phase undefined!!!")
		}

	}
	return nil
}

//任务阶段的转换
func (coordinator *Coordinator) toNextPhase() {
	if coordinator.Phase == mapPhase {
		coordinator.makeReduceTasks()
		coordinator.Phase = reducePhase
		//coordinator.Phase = allDone
	} else if coordinator.Phase == reducePhase {
		coordinator.Phase = allDone
	}
}
func (taskMetaHolder *TaskMetaHolder) checkTaskDone() bool {
	//根据当前所有任务的阶段数量来判断任务所处的总阶段
	var (
		mapDoneNum    = 0
		mapUnDoNum    = 0
		reduceDoneNum = 0
		reduceUndoNum = 0
	)
	//根据当前map情况完善任务数量信息
	for _, v := range taskMetaHolder.MetaMap {
		if v.TaskAdr.TaskType == mapTask {
			if v.state == done {
				mapDoneNum++
			} else {
				mapUnDoNum++
			}
		} else if v.TaskAdr.TaskType == reduceTask {
			if v.state == done {
				reduceDoneNum++
			} else {
				reduceUndoNum++
			}
		}
	}
	//已做完map任务 等待reduce任务
	if (mapDoneNum > 0 && mapUnDoNum == 0) && (reduceDoneNum == 0 && reduceUndoNum == 0) {
		return true
	} else {
		//正在做reduce任务
		if reduceDoneNum > 0 && reduceUndoNum == 0 {
			return true
		}
	}
	//所有任务已完成
	return false

}

//MetaMap map[int]*TaskMetaInfo
//type TaskMetaInfo struct{
//	state State //任务的状态
//	TaskAdr *Task
//当chan中存在map任务 判断任务的状态 是否正在工作 并调用该任务进行
func (taskMetaHolder *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := taskMetaHolder.MetaMap[taskId]
	if !ok || taskInfo.state != waiting {
		return false
	}
	taskInfo.state = working
	taskInfo.StartTime=time.Now()
	return true
}
