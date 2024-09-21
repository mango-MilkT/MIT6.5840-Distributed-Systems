package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// === what definitions???
	MapTaskTitles    []string
	MapTaskContents  []string
	MapTaskStates    []int8 // 0 for idle, 1 for being processed, 2 for successfully done
	ReduceTaskStates []int8
	AllTaskDone      bool
	Mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskRPC(args *AskArgs, reply *AskReply) error {
	reply.TaskAllDone = true
	reply.TaskIndex = -1
	reply.TaskTitle = ""
	reply.TaskContent = ""
	if args.TaskType == "Map" {
		c.Mu.Lock()
		for index, state := range c.MapTaskStates {
			if state == 0 {
				c.MapTaskStates[index] = 1
				reply.TaskAllDone = false
				reply.TaskIndex = index
				reply.TaskTitle = c.MapTaskTitles[index]
				reply.TaskContent = c.MapTaskContents[index]
				break
			} else if state == 1 {
				reply.TaskAllDone = false
			}
		}
		c.Mu.Unlock()
	} else if args.TaskType == "Reduce" {
		c.Mu.Lock()
		for index, state := range c.ReduceTaskStates {
			if state == 0 {
				c.ReduceTaskStates[index] = 1
				reply.TaskAllDone = false
				reply.TaskIndex = index
				break
			} else if state == 1 {
				reply.TaskAllDone = false
			}
		}
		c.AllTaskDone = reply.TaskAllDone
		c.Mu.Unlock()
	}
	return nil
}

func (c *Coordinator) ReportRPC(args *ReportArgs, reply *ReportReply) error {
	if args.TaskType == "Map" {
		c.Mu.Lock()
		if args.TaskSucceed {
			c.MapTaskStates[args.TaskIndex] = 2
		} else {
			c.MapTaskStates[args.TaskIndex] = 0
		}
		c.Mu.Unlock()
	} else if args.TaskType == "Reduce" {
		c.Mu.Lock()
		if args.TaskSucceed {
			c.ReduceTaskStates[args.TaskIndex] = 2
		} else {
			c.ReduceTaskStates[args.TaskIndex] = 0
		}
		c.Mu.Unlock()
	}

	return nil
}

// === the RPC argument and reply types are defined in rpc.go.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// === crash finder
func monitor(c *Coordinator) {
	var last_states []int8
	var cur_states []int8
	var time_records []int8
	last_states = append(c.MapTaskStates, c.ReduceTaskStates...)
	for i := 0; i < len(last_states); i++ {
		time_records = append(time_records, 0)
	}
	for {
		cur_states = append(c.MapTaskStates, c.ReduceTaskStates...)
		for index, last_state := range last_states {
			if last_state == 1 && cur_states[index] == 1 {
				time_records[index] += 1
				if time_records[index] > 10 {
					time_records[index] = 0
					c.Mu.Lock()
					if index < len(c.MapTaskStates) {
						c.MapTaskStates[index] = 0
					} else {
						c.ReduceTaskStates[index-len(c.MapTaskStates)] = 0
					}
					c.Mu.Unlock()
				}
			} else if cur_states[index] == 0 || cur_states[index] == 2 {
				time_records[index] = 0
			}
		}
		last_states = cur_states
		time.Sleep(time.Second)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// === to find out if the job has finished

	ret = c.AllTaskDone

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// === read the files and add them to Tasks list
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		c.MapTaskTitles = append(c.MapTaskTitles, filename)
		c.MapTaskContents = append(c.MapTaskContents, string(content))
		c.MapTaskStates = append(c.MapTaskStates, 0)
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskStates = append(c.ReduceTaskStates, 0)
	}
	c.server()
	go monitor(&c) // === start a thread to find potential crash
	return &c
}
