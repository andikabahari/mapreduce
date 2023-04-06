package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := TaskArgs{PrevTask: Task{Idx: -1}}

	for {
		reply := assignTask(args)

		switch reply.NewTask.Phase {
		case PhaseMap:
			file, err := os.Open(reply.NewTask.Files[0])
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatal(err)
			}

			kva := mapf(reply.NewTask.Files[0], string(content))
			buckets := buckets(kva, reply.NReduce)

			onames := make([]string, 0)
			for i, kva := range buckets {
				oname := fmt.Sprintf("mr-%d-%d", reply.NewTask.Idx, i)
				ofile, _ := os.Create(oname)
				defer ofile.Close()

				enc := json.NewEncoder(ofile)
				for _, kv := range kva {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}

				onames = append(onames, oname)
			}

			args.PrevTask = reply.NewTask
			args.NewReduceFiles = onames
		case PhaseReduce:
			intermediate := kva(reply.NewTask.Files)
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.NewTask.Idx)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := make([]string, 0)
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			args.PrevTask = reply.NewTask
			args.NewReduceFiles = make([]string, 0)
		case PhaseWait:
			time.Sleep(200 * time.Millisecond)
		case PhaseShutdown:
			return
		default:
			log.Fatal("unknown phase")
		}
	}
}

// returns nReduce buckets of intermediate key-values
func buckets(kva []KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		buckets[idx] = append(buckets[idx], kv)
	}
	return buckets
}

// returns intermediate key-values from files
func kva(files []string) []KeyValue {
	kva := make([]KeyValue, 0)
	for _, name := range files {
		file, err := os.Open(name)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func assignTask(args TaskArgs) TaskReply {
	var reply TaskReply
	if ok := call("Coordinator.AssignTask", args, &reply); !ok {
		log.Println("call failed")
		os.Exit(0)
	}
	return reply
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
