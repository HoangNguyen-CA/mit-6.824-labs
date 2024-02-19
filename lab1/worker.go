package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"regexp"
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
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	regRes, err := callRegister()
	if err != nil {
		log.Fatal(err)
	}

	runner := &Runner{
		id:      regRes.AssignedId,
		nReduce: regRes.NReduce,
		mapf:    mapf,
		reducef: reducef,
	}

	for {
		if exit := callReady(runner); exit {
			break
		}
	}
}

// register the worker with the coordinator
func callRegister() (RegisterResponse, error) {
	args := RegisterRequest{}

	reply := RegisterResponse{}

	if ok := call("Coordinator.RegisterHandler", &args, &reply); !ok {
		return RegisterResponse{}, fmt.Errorf("error with registering worker")
	}

	return reply, nil
}

// call the coordinator to get a task
func callReady(r *Runner) bool {
	args := ReadyRequest{r.id}

	reply := ReadyReply{}

	if ok := call("Coordinator.ReadyHandler", &args, &reply); !ok {
		log.Fatal("callReady RPC failed")
	}

	taskId := reply.TaskNumber

	if reply.TaskType == MapTaskType {
		intermediate := r.runMap(reply.FileName)
		r.storeIntermediate(taskId, intermediate)
		CallDoneMap(r)
	}

	if reply.TaskType == ReduceTaskType {
		r.runReduce(taskId)
		CallDoneReduce(r)
	}

	if reply.TaskType == WaitTaskType {
		time.Sleep(1 * time.Second)
	}

	if reply.TaskType == ExitTaskType {
		return true
	}

	return false
}

func CallDoneMap(r *Runner) {
	args := DoneMapRequest{r.id}

	reply := DoneMapReply{}

	if ok := call("Coordinator.DoneMapHandler", &args, &reply); !ok {
		log.Fatalf("callDoneMap RPC failed")
	}
}

func CallDoneReduce(r *Runner) {
	args := DoneMapRequest{r.id}

	reply := DoneMapReply{}

	if ok := call("Coordinator.DoneReduceHandler", &args, &reply); !ok {
		log.Fatalf("callDoneReduce RPC failed")
	}
}

type Runner struct {
	id      int
	nReduce int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// read input file,
// pass it to Map,
// accumulate the intermediate Map output.
func (r *Runner) runMap(inputFile string) [][]KeyValue {
	filename := inputFile
	res := make([][]KeyValue, r.nReduce)
	for i := 0; i < r.nReduce; i++ {
		res[i] = []KeyValue{}
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := r.mapf(filename, string(content))
	for _, kv := range kva {
		hash := ihash(kv.Key) % r.nReduce
		res[hash] = append(res[hash], kv)
	}

	return res
}

// store intermediate results from mapping in form of mr-<task_id>-<i> files
func (r *Runner) storeIntermediate(taskId int, intermediate [][]KeyValue) {
	for i, kva := range intermediate {
		file, err := os.CreateTemp("", fmt.Sprintf("tmpint-%v", taskId))
		if err != nil {
			log.Fatalf("cannot create intermediate file")
		}
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode intermediate file")
			}
		}

		// atomic rename to prevent partial results
		os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", taskId, i))
	}
}

// runs the reduce process
func (r *Runner) runReduce(taskId int) {
	files, err := getFilesWithSuffix(fmt.Sprint(taskId))
	if err != nil {
		log.Fatalf("cannot get files with suffix")
	}

	values := make([]KeyValue, 0)

	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			values = append(values, kv)
		}
	}
	sort.Sort(ByKey(values))

	ofile, err := os.CreateTemp("", fmt.Sprintf("tmprdc-%v", taskId))

	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	i := 0
	for i < len(values) {
		j := i + 1
		for j < len(values) && values[j].Key == values[i].Key {
			j++
		}
		temp := []string{}
		for k := i; k < j; k++ {
			temp = append(temp, values[k].Value)
		}
		output := r.reducef(values[i].Key, temp)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", values[i].Key, output)

		i = j
	}
	ofile.Close()

	// atomic rename to prevent partial results
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%v", taskId))
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

// helper function returning all intermediate files with a given suffix
func getFilesWithSuffix(suffix string) ([]string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var result []string
	pattern := fmt.Sprintf(`-%s$`, regexp.QuoteMeta(suffix))

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		matched, err := regexp.MatchString(pattern, file.Name())
		if err != nil {
			return nil, err
		}

		if matched {
			result = append(result, file.Name())
		}
	}

	return result, nil
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
