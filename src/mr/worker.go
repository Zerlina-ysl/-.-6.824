package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
// 键值对,表示文件内容中的 <字符，数量>

type KeyValue struct {
	Key   string
	Value string
}
type SortedKey []KeyValue


// for sorting by key.
func (a SortedKey) Len() int           { return len(a) }
func (a SortedKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	//New32a 返回一个新的 32 位 FNV-1a hash.Hash。它的 Sum 方法将以大端字节顺序排列值。
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DoMapTask(mapf func(string,string)[]KeyValue,response *Task){
	//intermediate是kv对组合
	var intermediate []KeyValue
	filename := response.FileSlice[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		}
	file.Close()
	//通过map函数得到的v是1
	intermediate = mapf(filename, string(content))
	//之前使用命令行进行实现 可能存在多组数据 因此for循环遍历+append
	//intermediate = append(intermediate, kva...)
	//创建nreduce个二维切片
	rn:=response.ReducerNum
	//每个reduce任务对应一个数组 里面存放的是
	HashedKV := make([][]KeyValue,rn)
	for _,kv := range intermediate{
		//将intermediate传递至reduce函数
		//根据kv.key->单词来分配负责reduce的结点
		HashedKV[ihash(kv.Key)%rn]=append(HashedKV[ihash(kv.Key)%rn],kv)
	}
	for i:=0;i<rn;i++{
		//中间文件的合理命名约定是 mr-X-Y，其中 X 是 Map 任务编号，Y 是 reduce 任务编号。
		//reduce调用是通过一个划分函数（例如hash(key) mod R)将中间key空间划分为R块来分布运行
		oname := "mr-tmp-"+strconv.Itoa(response.TaskId)+"-"+strconv.Itoa(i)
		//中间文件
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, okv := range HashedKV[i] {
			 err:=enc.Encode(&okv)
			 if err!=nil{
			 	fmt.Println("编码过程出现error")
			 	return
			 }
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string,response *Task){
//1. 洗牌 得到排序好的kv数组
	//每开启一个map，id就会增加
	//每开启一个reduce,id就会增加
	//表示是当前的第几个任务
	reduceFileNum := response.TaskId
	//对之前的tmp文件进行洗牌排序
	intermediate := shuffle(response.FileSlice)
	dir,_:=os.Getwd()
	//TempFile 在目录 dir 中新建一个临时文件，打开文件进行读写，并返回生成的 os.File。
	tmpFile,err:=ioutil.TempFile(dir,"mr-tmp-*")
	if err!=nil{
		log.Fatal("failed to create temp file",err)
	}

	// 2. 根据排序数组定向输出文件
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 2. 将相同key值使用values数组盛放 并且输入reduce函数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()
	//mr-out-reduceFileNum
	fn:=fmt.Sprintf("mr-out-%d",reduceFileNum)
	os.Rename(tmpFile.Name(),fn)

}
func shuffle(files []string)[]KeyValue {
	var kva []KeyValue
	for _,filepath :=range files{
		file,_:=os.Open(filepath)
		dec:=json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// sort.Sort(ByKey(intermediate))
	sort.Sort(SortedKey(kva))
	return kva
}
func callDone(f *Task) Task{
	args:=f
	reply:=Task{}
	//完成任务
	ok:=call("Coordinator.MarkFinished",&args,&reply)
	if ok {
		fmt.Println(reply)
	} else {

		fmt.Printf("call failed!\n")
	}
	return reply
}

// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//使用后keepFlag判断任务状态
	keepFlag:=true
	for keepFlag{
		task:=GetTask()
		switch task.TaskType {
		case mapTask:
			{
				//1个map和多个reduce
				DoMapTask(mapf, &task)
				fmt.Println("*************Map***************")
				callDone(&task)
			}
		case waitingTask:
			{
				fmt.Println("all tasks is in progress,please wait...")
				time.Sleep(time.Second*5)
			}
		case reduceTask:
			{
				DoReduceTask(reducef,&task)
				callDone(&task)
			}
		case exitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("Task abous:[", task.TaskId, "] is terminated...")
				fmt.Println("all tasks are Done,will be exiting")
				keepFlag = false
			}
		}
	}
	time.Sleep(time.Second)
}

//-----获取任务 类比CallExample()
func GetTask() Task {
	//args是需要传递的参数，因为该rpc通信是为了获取任务，因此定义为空。
	args:=TaskArgs{}
	//reply是rpc通信后得到的结果，是GetTask()需要获取并分配的(map/reduce)任务
	reply:=Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		fmt.Println(reply)
		fmt.Println("worker get",reply.TaskType,"task:id[",reply.TaskId,"]")
	} else {
		fmt.Println("call failed!\n")
		}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.

//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

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
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
