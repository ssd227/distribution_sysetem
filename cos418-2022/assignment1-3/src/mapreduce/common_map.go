package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	// Use checkError to handle errors.
	// STEP1: open inFile and read content as string
	file, err := os.Open(inFile)
	if err != nil {
		log.Fatalf("cannot open %v", inFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inFile)
	}
	file.Close()

	// STEP2: map k-v(filename-content), get new k-v slice
	kva := mapF(inFile, string(content))

	// STEP3: split to nreduce Part
	rid2KVs := make(map[int](map[string][]string))
	for _, kv := range kva {
		rId := int(ihash(kv.Key)) % nReduce
		if rid2KVs[rId] == nil {
			rid2KVs[rId] = make(map[string][]string)
		}
		rid2KVs[rId][kv.Key] = append(rid2KVs[rId][kv.Key], kv.Value)
	}

	// STEP4:save each part in file with json formate
	for reduceTaskId, k_vli := range rid2KVs {
		reduceFileName := reduceName(jobName, mapTaskNumber, reduceTaskId)

		outFile, _ := os.OpenFile(reduceFileName, os.O_RDWR|os.O_CREATE, 0666)
		// outFile, _ := os.OpenFile(reduceFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		defer outFile.Close()
		encoder := json.NewEncoder(outFile)

		err := encoder.Encode(&k_vli)
		if err != nil {
			log.Fatalf("[doMap] json.encode error in outputfile %v",
				outFile)
		}
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
