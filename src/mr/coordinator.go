package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	stageInit   = 1
	stageMap    = 2
	stageReduce = 3
)

type Coordinator struct {
	// Your definitions here.
	done              bool
	stage             int
	intermediateFiles [][]string

	indexToMap    int
	indexToReduce int

	markMapTask    map[int]bool
	markReduceTask map[int]bool

	mtx sync.Mutex

	splitNum  int
	reduceNum int
	files     []string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	reply.Stage = c.stage

	if c.done {
		reply.Done = true
		return nil
	}

	if c.stage == stageMap {
		if c.indexToMap >= c.splitNum {
			reply.Wait = true
			// log.Printf("indexToMap:%v, split num: %v, no task for map \n", c.indexToMap, c.splitNum)
			return nil
		}
		reply.SplitIndex = c.indexToMap
		reply.SplitFilePath = c.files[reply.SplitIndex]
		reply.NReduce = c.reduceNum

		log.Printf("map SplitIndex:%v, SplitFilePath: %v,\n", reply.SplitIndex, reply.SplitFilePath)

		c.indexToMap += 1
		return nil
	} else if c.stage == stageReduce {
		if c.indexToReduce >= c.reduceNum {
			// log.Printf("indexToMap:%v, split num: %v, no task for reduce \n", c.indexToMap, c.splitNum)
			reply.Wait = true
			return nil
		}

		reply.ReduceIndex = c.indexToReduce
		reply.SplitNum = c.splitNum

		log.Printf("reduce ReduceIndex:%v, SplitNum: %v,\n", reply.ReduceIndex, reply.SplitNum)

		c.indexToReduce += 1
	} else {
		err := fmt.Errorf("invalid internal stage: %v", c.stage)
		reply.Err = err.Error()
	}

	return nil
}

func (c *Coordinator) allMarked(tomark map[int]bool) bool {
	for _, v := range tomark {
		if !v {
			return v
		}
	}
	return true
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	stage := args.Stage

	if stage == stageMap {
		if k := c.markMapTask[args.SplitIndex]; !k {
			c.markMapTask[args.SplitIndex] = true

			if c.allMarked(c.markMapTask) {
				log.Println("stage from map -----------> reduce")
				c.stage = stageReduce
			}
			return nil
		}
	}

	if stage == stageReduce {
		if k := c.markReduceTask[args.PartitionIndex]; !k {
			c.markReduceTask[args.PartitionIndex] = true

			if c.allMarked(c.markReduceTask) {
				c.done = true
			}
			return nil
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("files: %v, nReduce:%v \n", files, nReduce)

	c := Coordinator{}

	// Your code here.
	c.stage = stageInit

	for i := 0; i < len(files); i++ {
		c.intermediateFiles = append(c.intermediateFiles, make([]string, 0, nReduce))
	}

	c.markMapTask = make(map[int]bool, 0)
	for i := 0; i < len(files); i++ {
		c.markMapTask[i] = false
	}

	c.markReduceTask = make(map[int]bool, 0)
	for i := 0; i < nReduce; i++ {
		c.markReduceTask[i] = false
	}

	c.stage = stageMap
	c.splitNum = len(files)
	c.reduceNum = nReduce
	c.files = files

	c.server()
	return &c
}
