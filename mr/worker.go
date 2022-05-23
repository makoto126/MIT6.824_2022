package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const interFileTmpl = "mr-%d-%d"
const outFileTmpl = "mr-out-%d"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func atomicWriteFile(filename string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(filename), ".tmp-"+filepath.Base(filename))
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		f.Close()
		return io.ErrShortWrite
	}
	if err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filename)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	req := Request{Report: Nothing}

outer:
	for {
		reply := Reply{}
		err = c.Call("Coordinator.Handler", &req, &reply)
		if err != nil {
			log.Println(err)
			continue
		}
		switch reply.Action {
		case Map:
			doMap(reply.Id, reply.N, reply.Filename, mapf)
			req.Id = reply.Id
			req.Report = MapDone
		case Reduce:
			doReduce(reply.Id, reply.N, reducef)
			req.Id = reply.Id
			req.Report = ReduceDone
		case Exit:
			break outer
		default:
			log.Printf("unkown action %d", reply.Action)
		}
	}

}

func doMap(mid int, nReduce int, filename string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kvs := mapf(filename, string(content))

	sort.Sort(ByKey(kvs))

	var rid int
	kvDict := map[int][]KeyValue{}
	i := 0
	for i < len(kvs) {
		j := i + 1

		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}

		rid = ihash(kvs[i].Key) % nReduce

		kvDict[rid] = append(kvDict[rid], kvs[i:j]...)

		i = j
	}

	var data []byte
	for rid, kv := range kvDict {
		data, err = json.Marshal(kv)
		if err != nil {
			log.Fatalf("json encode failed %v", kv)
		}
		err = atomicWriteFile(fmt.Sprintf(interFileTmpl, mid, rid), data)
		if err != nil {
			log.Fatalf("atomic write file failed")
		}
	}
}

func doReduce(rid int, nMap int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for mid := 0; mid < nMap; mid++ {
		f, err := os.Open(fmt.Sprintf(interFileTmpl, mid, rid))
		if err != nil {
			continue
		}
		var kvs []KeyValue
		err = json.NewDecoder(f).Decode(&kvs)
		if err != nil {
			log.Fatalf("json decode failed %s", f.Name())
		}
		kva = append(kva, kvs...)
	}

	sort.Sort(ByKey(kva))

	buffer := new(bytes.Buffer)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(buffer, "%v %v\n", kva[i].Key, output)

		i = j
	}

	err := atomicWriteFile(fmt.Sprintf(outFileTmpl, rid), buffer.Bytes())
	if err != nil {
		log.Fatalf("atomic write file failed")
	}
}
