package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueList struct {
	Key    string
	Values []string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// TODO: ensure closing opened files
	// Your worker implementation here.
	for {
		req := EmptyMsg{}
		rsp := TaskRsp{}
		ok := call("Coordinator.GetTask", &req, &rsp)
		interDir := "./intermediate"
		if ok {
			if rsp.TaskType == 0 {
				fmt.Println("worker exit.")
				break
			} else if rsp.TaskType == 1 {
				filename := rsp.FileName
				file, err := os.Open(filename)
				if err != nil {
					fmt.Printf("cannot open %v\n", filename)
					continue
				}
				content, err := io.ReadAll(file)
				file.Close()
				if err != nil {
					fmt.Printf("cannot read %v\n", filename)
					continue
				}
				intermediate := mapf(filename, string(content))

				if exist, err := PathExists(interDir); !exist {
					if err != nil {
						log.Fatalf("os error\n")
					} else {
						os.Mkdir(interDir, os.ModePerm)
					}
				}
				tmpfiles := []*os.File{}
				for i := 0; i < rsp.CommonArg; i++ {
					ifile, err := ioutil.TempFile(interDir, fmt.Sprintf("inter-%d", i))
					if err != nil {
						fmt.Println("create tmp intermediate file failed!")
						break
					}
					tmpfiles = append(tmpfiles, ifile)
				}
				if len(tmpfiles) != rsp.CommonArg {
					fmt.Println("not enough tmp files")
					continue
				}

				encoders := []*json.Encoder{}
				for _, f := range tmpfiles {
					encoders = append(encoders, json.NewEncoder(f))
				}

				for _, p := range intermediate {
					hid := ihash(p.Key) % rsp.CommonArg
					enc := encoders[hid]
					enc.Encode(&p)
				}

				for rid, f := range tmpfiles {
					os.Rename(f.Name(), fmt.Sprintf(interDir+"/mr-%d-%d", rsp.TaskId, rid))
					f.Close()
				}
			} else {
				intermediate := []KeyValue{}
				interfomat := "intermediate/mr-%d-" + strconv.Itoa(rsp.TaskId)
				flag := true
				for i := 0; i < rsp.CommonArg; i++ {
					fileName := fmt.Sprintf(interfomat, i)
					f, err := os.Open(fileName)
					if err != nil {
						fmt.Printf("open inter file %s failed\n", fileName)
						flag = false
						break
					}
					dec := json.NewDecoder(f)
					for {
						var kvs KeyValue
						if err := dec.Decode(&kvs); err != nil {
							break
						}
						intermediate = append(intermediate, kvs)
					}
				}
				if !flag {
					continue
				}
				sort.Sort(ByKey(intermediate))

				ofile, err := ioutil.TempFile(interDir, "reduce")
				if err != nil {
					fmt.Println("create reduce tmp file failed:", err)
					continue
				}

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

				os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", rsp.TaskId))
				ofile.Close()
			}

			if rsp.TaskType != 0 {
				// notify coordinator
				notify(&rsp.Task)
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func notify(req interface{}) {
	ok := call("Coordinator.Notify", req, nil)
	if !ok {
		fmt.Println("notify failed!")
	}
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
