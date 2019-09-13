package mapreduce

import (
    "hash/fnv"
    "io/ioutil"
    "encoding/json"
    //"fmt"
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
    //fmt.Printf("MAP\n")
    dat, err := ioutil.ReadFile(inFile)
    checkError(err)
    
    //fmt.Printf("MAP: Calling mapF infile = [%s] with dat = [%s]\n", inFile, string(dat))
    intermediateKeyValues := mapF(inFile, string(dat))
    //fmt.Printf("intermediateKeyValues[0].[key]/[value] = [%s][%s] \n", intermediateKeyValues[0].Key, intermediateKeyValues[0].Value)
    

    /* Now lets arrange key/value pairs per their corresponding bucket */
    kvFileList := make([]KeyValueFileList, nReduce)
    /* Initialize the struct */
    for i := 0; i < nReduce; i++ {
      kvFileList[i].ofile = reduceName(jobName, mapTaskNumber, i)
      var kvArr []KeyValue
      kvFileList[i].kv = kvArr
    }
    
    /* Assign each key/value pair to its corresponding bucket */
    for i := 0 ; i < len(intermediateKeyValues); i++ {
      /* Get the bucket number for this key/value */
      r := int(ihash(intermediateKeyValues[i].Key)%uint32(nReduce))
      
      //fmt.Printf("Bucket[%d] value[%s] nreduce[%d]\n", r, intermediateKeyValues[i], nReduce)
      /* Now append key/value into array for that corresponding bucket */
      kvFileList[r].kv = append(kvFileList[r].kv, intermediateKeyValues[i])
    }
    
    
    /* Write each key/value pair to its corresponding output file */
    for i := 0; i < nReduce; i++ {
    
      /* Open target file */
      //fmt.Printf("MAP: Result File [%s]\n", kvFileList[i].ofile)
      //fmt.Printf("MAP: Marshalling array [%v]\n", kvFileList[i].kv)
      
      /* Marshall the key/value pairs array */
      kv_m, err := json.Marshal(kvFileList[i].kv)
      
      /* Write the result */
      err = ioutil.WriteFile(kvFileList[i].ofile, kv_m, 0644)
      checkError(err)
    }
}

/* This structure is used in order to organize all key/value pairs before writting them to their output files */
type KeyValueFileList struct {
    ofile   string
    kv    []KeyValue
}

func ihash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}
