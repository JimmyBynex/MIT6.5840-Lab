package mr

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
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

type shuffleKeyValue struct {
	Key    string
	Values []string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		//rpc获取task
		getTaskArgs := GetTaskArgs{}

		getTaskReply := GetTaskReply{}

		ok := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
		if !ok {
			fmt.Printf("call Coordinator.GetTask failed!\n")
			return
		}

		//避免忙等
		if getTaskReply.Wait {
			time.Sleep(time.Second)
			continue
		}

		switch getTaskReply.TaskType {
		case mapTask:
			content, err := os.ReadFile(getTaskReply.File)
			if err != nil {
				fmt.Printf("read file:%v\n", err)
				return
			}
			intermediates := mapf(getTaskReply.File, string(content))

			m := getTaskReply.Id
			n := getTaskReply.NReduce
			tmpFileName := make([]string, n)
			tmpFile := make([]*os.File, n)
			for i := range n {
				file, err := os.CreateTemp("", "mr-tmp-*")
				defer file.Close()
				defer os.Remove(file.Name())
				if err != nil {
					fmt.Printf("open temp file:%v\n", err)
					return
				}
				tmpFile[i] = file
				tmpFileName[i] = file.Name()
			}

			for _, kv := range intermediates {
				r := ihash(kv.Key) % n
				data, err := json.Marshal(kv)
				if err != nil {
					fmt.Printf("marshal intermidate key-value:%v\n", err)
					return
				}
				_, err = tmpFile[r].Write(data)
				if err != nil {
					fmt.Printf("append tmpfile: %v\n", err)
					return
				}
				_, err = tmpFile[r].Write([]byte("\n"))
				if err != nil {
					fmt.Printf("append newline: %v\n", err)
					return
				}
			}

			// 关闭所有文件，确保数据写入磁盘
			for i := range n {
				tmpFile[i].Close()
			}

			for i := range n {
				mr := fmt.Sprintf("mr-%d-%d", m, i)
				if err = os.Rename(tmpFileName[i], mr); err != nil {
					tmpFile[i].Close()
					fmt.Printf("rename tmpFile:%v\n", err)
					return
				}
			}

			finishTaskArgs := FinishTaskArgs{TaskType: mapTask, Id: m}
			FinishTaskReply := FinishTaskReply{}
			if ok := call("Coordinator.FinishTask", &finishTaskArgs, &FinishTaskReply); !ok {
				fmt.Print("call Coordinator.FinishTask failed\n")
				return
			}

		case reduceTask:
			r := getTaskReply.Id
			filePattern := fmt.Sprintf("mr-*-%d", r)
			names, err := filepath.Glob(filePattern)
			if err != nil {
				fmt.Printf("glob failed: %v\n", err)
				return
			}

			// 过滤出符合 mr-{数字}-{数字} 格式的文件
			validNames := []string{}
			for _, name := range names {
				// 检查格式：mr-数字-数字
				var m, r int
				if _, err := fmt.Sscanf(filepath.Base(name), "mr-%d-%d", &m, &r); err == nil {
					validNames = append(validNames, name)
				}
			}
			names = validNames
			//shuffle
			kva := []KeyValue{}
			for _, name := range names {
				file, err := os.Open(name)
				if err != nil {
					fmt.Printf("open file failed: %v\n", err)
					return
				}

				// 检查文件是否为空，跳过空文件
				stat, err := file.Stat()
				if err != nil {
					file.Close()
					fmt.Printf("stat file failed: %v\n", err)
					return
				}
				if stat.Size() == 0 {
					file.Close()
					continue
				}

				dec := json.NewDecoder(file)

				for {
					var kv KeyValue
					err := dec.Decode(&kv)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						fmt.Printf("decoded kv failed from file %s: %v\n", name, err)
						return
					}
					kva = append(kva, kv)
				}
			}

			slices.SortFunc(kva, func(i, j KeyValue) int {
				return cmp.Compare(i.Key, j.Key)
			})

			shuffleKv := []shuffleKeyValue{}
			nKva := len(kva)
			for i := 0; i < nKva; {
				j := i + 1
				for j < nKva && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				shuffleKv = append(shuffleKv, shuffleKeyValue{Key: kva[i].Key, Values: values})
				i = j
			}

			//reduce
			outs := []KeyValue{}
			for _, kvs := range shuffleKv {
				out := reducef(kvs.Key, kvs.Values)
				outs = append(outs, KeyValue{Key: kvs.Key, Value: out})
			}

			//out
			file, err := os.CreateTemp("", "out-tmp-*")
			defer file.Close()
			defer os.Remove(file.Name())
			if err != nil {
				fmt.Printf("create tempFile:%v\n", err)
				return
			}
			for _, out := range outs {
				if _, err := fmt.Fprintf(file, "%v %v\n", out.Key, out.Value); err != nil {
					fmt.Printf("write output failed:%v\n", err)
					return
				}
			}
			file.Close()
			outName := fmt.Sprintf("mr-out-%d", r)
			os.Rename(file.Name(), outName)

			finishTaskArgs := FinishTaskArgs{TaskType: reduceTask, Id: r}
			FinishTaskReply := FinishTaskReply{}
			if ok := call("Coordinator.FinishTask", &finishTaskArgs, &FinishTaskReply); !ok {
				fmt.Print("call Coordinator.FinishTask failed\n")
				return
			}

		case doneTask:
			return
		}
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
