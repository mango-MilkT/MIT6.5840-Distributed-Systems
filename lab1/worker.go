package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// === The map part of your worker can use the ihash(key) function (in worker.go) to pick the reduce task for a given key.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int((h.Sum32() & 0x7fffffff) % 10)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// for sorting by key hash.
type ByKeyHash []KeyValue

// for sorting by key hash.
func (a ByKeyHash) Len() int           { return len(a) }
func (a ByKeyHash) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeyHash) Less(i, j int) bool { return ihash(a[i].Key) < ihash(a[j].Key) }

// 判断所给路径文件/文件夹是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	//isnotexist来判断，是不是不存在的错误
	if os.IsNotExist(err) { //如果返回的错误类型使用os.isNotExist()判断为true，说明文件或者文件夹不存在
		return false, nil
	}
	return false, err //如果有错误了，但是不是不存在的错误，所以把这个错误原封不动的返回
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		alldone, index, title, content := AskMapTask()
		if alldone {
			break
		} else {
			if index != -1 {
				succeed := true
				kva := mapf(title, content)
				sort.Sort(ByKeyHash(kva))
				// for _, kva := range kva[:3] {
				// 	fmt.Printf("%v, %v, %d\n", kva.Key, kva.Value, ihash(kva.Key))
				// }

				for i, j, k := 0, 0, 0; i < 10 && j < len(kva); i++ {
					var interfile *os.File
					filename := fmt.Sprintf("mr-inter-%d-%d", index, i)
					exist, err := PathExists(filename)
					if exist {
						interfile, _ = os.Open(filename)
					} else {
						if err == nil {
							interfile, _ = os.Create(filename)
						} else {
							break
						}
					}
					k = j
					for ; k < len(kva); k++ {
						if ihash(kva[k].Key) != i {
							break
						}
					}
					enc := json.NewEncoder(interfile)
					for _, kv := range kva[j:k] {
						err := enc.Encode(&kv)
						if err != nil {
							succeed = false
							break
						}
					}
					j = k
					interfile.Close()
				}

				ReportMapTask(index, succeed)
			}
		}
		time.Sleep(time.Second)
	}

	for {
		alldone, index, _ := AskReduceTask()
		if alldone {
			break
		} else {
			if index != -1 {
				succeed := true
				intermediate := []KeyValue{}

				patern := fmt.Sprintf("mr-inter-*-%d", index)
				matches, err := filepath.Glob(patern)
				if err != nil {
					fmt.Println(err)
					return
				}

				// 遍历所有匹配的文件
				for _, match := range matches {
					// 打开文件
					file, err := os.Open(match)
					if err != nil {
						fmt.Printf("Error opening file %s: %s\n", match, err)
						continue
					}

					// 这里可以添加读取文件内容的代码
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}

					file.Close()
				}

				sort.Sort(ByKey(intermediate))

				oname := fmt.Sprintf("mr-out-%d", index)
				ofile, _ := os.Create(oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
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
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				ofile.Close()

				ReportReduceTask(index, succeed)
			}
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	// fmt.Printf("worker exit\n")

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func AskMapTask() (bool, int, string, string) {
	args := AskArgs{}
	args.TaskType = "Map"

	reply := AskReply{}

	ok := call("Coordinator.AskRPC", &args, &reply)
	if ok {
		if reply.TaskIndex != -1 {
			// fmt.Printf("%s\n", reply.TaskContent[:30])
		}
		return reply.TaskAllDone, reply.TaskIndex, reply.TaskTitle, reply.TaskContent
	} else {
		fmt.Printf("call failed!\n")
		return false, -1, "", ""
	}
}

func ReportMapTask(index int, succeed bool) {
	args := ReportArgs{}
	args.TaskType = "Map"
	args.TaskIndex = index
	args.TaskSucceed = succeed

	reply := ReportReply{}

	ok := call("Coordinator.ReportRPC", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}
}

func AskReduceTask() (bool, int, string) {
	args := AskArgs{}
	args.TaskType = "Reduce"

	reply := AskReply{}

	ok := call("Coordinator.AskRPC", &args, &reply)
	if ok {
		return reply.TaskAllDone, reply.TaskIndex, reply.TaskContent
	} else {
		fmt.Printf("call failed!\n")
		return false, -1, ""
	}
}

func ReportReduceTask(index int, succeed bool) {
	args := ReportArgs{}
	args.TaskType = "Reduce"
	args.TaskIndex = index
	args.TaskSucceed = succeed

	reply := ReportReply{}

	ok := call("Coordinator.ReportRPC", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
