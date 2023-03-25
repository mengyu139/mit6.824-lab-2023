package mr

import (
	"bufio"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// defer log.Println("exited worker .......")

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	for {
		time.Sleep(time.Millisecond * 100)

		task, err := GetTask(nil)
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		if task.Err != "" {
			log.Printf("get task failed, err: %v\n", task.Err)
			continue
		}
		if task.Wait {
			// log.Printf("stage:%v, wait ...\n", task.Stage)
			continue
		}

		if task.Done {
			// log.Println("done ...")
			return
		}

		stage := task.Stage
		if stage == stageMap {
			if err := processMap(ctx, mapf, task.SplitFilePath, task.SplitIndex, task.NReduce); err != nil {
				log.Fatal(err)
				return
			}
			if err := ReportTask(stage, task.SplitIndex); err != nil {
				log.Fatal(err)
				return
			}
		}

		if stage == stageReduce {
			if err := processReduce(ctx, reducef, task.SplitNum, task.ReduceIndex); err != nil {
				log.Fatal(err)
				return
			}
			if err := ReportTask(stage, task.ReduceIndex); err != nil {
				log.Fatal(err)
				return
			}
		}
	}

}

func heartBeat(ctx context.Context, g *sync.WaitGroup, stage, index int) {
	defer g.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		GetTask(&getParams{
			stage: stage,
			index: index,
		})
		time.Sleep(time.Millisecond * 300)
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func processMap(ctx context.Context, mapf func(string, string) []KeyValue, filename string, index int, nReduce int) error {
	// log.Printf("start map, split file: %v, index: %v, reduce: %v\n", filename, index, nReduce)

	g := sync.WaitGroup{}
	defer g.Wait()

	nctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g.Add(1)
	go heartBeat(nctx, &g, stageMap, index)

	// time.Sleep(time.Second * 2)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	ofiles := make([]*os.File, 0)

	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("%v-%v.txt", index, i)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		ofiles = append(ofiles, ofile)
	}

	// save intermediate result by partition
	for i := range kva {
		r := ihash(kva[i].Key) % nReduce
		f := ofiles[r]
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, kva[i].Value)
	}

	// log.Printf("start map, split file: %v, index: %v, reduce: %v, done ........\n", filename, index, nReduce)

	return nil
}

func processReduce(ctx context.Context, reducef func(string, []string) string, splitNum int, reduceIndex int) error {
	// log.Printf("start reduce, reduceIndex: %v\n", reduceIndex)

	g := sync.WaitGroup{}
	defer g.Wait()

	nctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g.Add(1)
	go heartBeat(nctx, &g, stageReduce, reduceIndex)

	intermediate := []KeyValue{}

	for i := 0; i < splitNum; i++ {
		filename := fmt.Sprintf("%v-%v.txt", i, reduceIndex)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return err
		}

		br := bufio.NewReader(file)
		for {
			a, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}

			// fmt.Println(string(a))
			cs := strings.Split(string(a), " ")
			kv := KeyValue{
				Key:   cs[0],
				Value: cs[1],
			}
			intermediate = append(intermediate, kv)

		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// dump to reduce file
	oname := fmt.Sprintf("mr-out-%v", reduceIndex)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-x.
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

	return nil
}

type getParams struct {
	stage int
	index int
}

func GetTask(params *getParams) (*TaskReply, error) {

	// declare an argument structure.
	args := TaskArgs{}
	if params != nil {
		args.Stage = params.stage
		args.Index = params.index
		args.HeartBeat = true
	}

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		return nil, fmt.Errorf("call GetTask failed!")
	}

}

func ReportTask(stage int, index int) error {
	// declare an argument structure.
	args := ReportTaskArgs{
		Stage: stage,
	}
	if stage == stageMap {
		args.SplitIndex = index
	} else if stage == stageReduce {
		args.PartitionIndex = index
	} else {
		err := fmt.Errorf("invalid argas, stage: %v, index: %v", stage, index)
		return err
	}

	// fill in the argument(s).

	// declare a reply structure.
	reply := ReportTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("call ReportTask failed!")
	}
	return nil

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
