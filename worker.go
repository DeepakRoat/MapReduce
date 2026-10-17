package MapReduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var workerID int = -1


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func acquireTask() TaskReply {
	task, ok := CallTask()

	if task.WorkerID==-1{
		handleExit(1)
	}
	if !ok {
		handleExit(0)
	}
	if(workerID==-1) {
		workerID=task.WorkerID
	}
	return task
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	// Your worker implementation here.

	var loop = true
	for (loop) {
		task := acquireTask()

		if (task.TaskType=="WAIT") {
			t := 1
			for {
				if task=acquireTask(); task.TaskType!="WAIT" {
					break
				}
				time.Sleep(time.Duration(t)*time.Second)
				t++
				t=max(t, 4)
			}
		}

		var err error

		switch task.TaskType {
		case "MAP":
			err = mapFunc(task, mapf)
		case "REDUCE":
			err = reduceFunc(task, reducef)
		case "EXIT":
			err = nil
			return true
		}
		if err!=nil {
			log.Printf("error occured: %v", err)
		}
	}

	return true
}

func handleExit(code int) {
	os.Exit(code)
}

func mapFunc(task TaskReply, mapf func(string, string) []KeyValue) error {
	file := task.Files[0]
	taskID := task.TaskID
	nReduce := task.NReduce

	f, err := os.Open(file)
	if err!=nil {
		CallReport("MAP", taskID, false)
		return fmt.Errorf("cannot open file %v: %v", file, err)
	}
	content, err := io.ReadAll(f)
	f.Close()
	if err!=nil {
		CallReport("MAP", taskID, false)
		return fmt.Errorf("cannot read file %v: %w", file, err)
	}

	kva := mapf(file, string(content))

	buckets := make([][]KeyValue, nReduce)

	for i:=0; i<nReduce; i++ {
		buckets[i] = make([]KeyValue, 0, int(3*len(kva)/nReduce))
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key)%nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for bucketIdx, kvList := range buckets {
		tmpFile, err := os.CreateTemp(".", "mr-map-tmp-*")
		if err!=nil {
			CallReport("MAP", taskID, false)
			return err
		}

		enc := json.NewEncoder(tmpFile)
		for _, kv:=range kvList {
			if err:=enc.Encode(&kv); err!= nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				CallReport("MAP", taskID, false)
				return err
			}
		}
		tmpFile.Close()

		finalName := fmt.Sprintf("mr-%d-%d", taskID, bucketIdx)
		if err:=os.Rename(tmpFile.Name(), finalName); err!=nil {
			os.Remove(tmpFile.Name())
			CallReport("MAP", taskID, false)
			return err
		}
	}
	CallReport("MAP", taskID, true)
	return nil
}

func reduceFunc(task TaskReply, reducef func(string, []string) string) error {
	var kva []KeyValue

	for _, filename := range task.Files {
		f, err := os.Open(filename)
		if err!=nil {
			CallReport("REDUCE", task.TaskID, false)
			return fmt.Errorf("cannot open intermidiate file %v: %w", filename, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err:=dec.Decode(&kv); err!=nil {
				if err == io.EOF {
					break
				}
				f.Close()
				CallReport("REDUCE", task.TaskID, false)
			}
			kva = append(kva, kv)
		}
		f.Close()
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key<kva[j].Key
	})
	
	tmpFile, err := os.CreateTemp(".", "mr-red-tmp-*")
	if err!=nil {
		CallReport("REDUCE", task.TaskID, false)
		return err
	}

	i:=0
	for i<len(kva) {
		j:=i+1
		for j<len(kva) && (kva[j].Key == kva[i].Key) {
			j++
		}
		
		values := make([]string, 0, (j-i))
		for k:=i; k<j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
		i=j
	}
	tmpFile.Close()

	outFile := fmt.Sprintf("mr-out-%d", task.TaskID)
	err = os.Rename(tmpFile.Name(), outFile)
	if err!=nil {
		os.Remove(tmpFile.Name())
		CallReport("REDUCE", task.TaskID, false)
		return err
	}

	CallReport("REDUCE", task.TaskID, true)
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallTask() (TaskReply, bool) {

	// declare an argument structure.
	args := TaskArgs{WorkerID: workerID}
	reply := TaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	
	if !ok {
		t := 1*time.Second
		for i:=0; i<3; i++ {
			if call("Coordinator.GetTask", &args, &reply) {
				return reply, true
			}
			time.Sleep(t)
			t++
		}
		return reply, false
	}
	return reply, true
}

func CallReport(taskType string, taskID int, status bool) bool {
	args := ReportTaskArgs{
		TaskType: taskType,
		TaskID: taskID,
		WorkerID: workerID,
		Status: status,
	}
	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)

	if !ok {
		t := 1*time.Second
		for i:=0; i<5; i++ {
			if call("Coordinator.ReportTask", &args, &reply) {
				return true
			}
			time.Sleep(t)
			t*=2
		}
		return false
	}
	return true
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
