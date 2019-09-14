
package mapreduce

import (
    "io/ioutil"
    "encoding/json"
    "sort"
    "strings"
    "os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
    jobName string, // the name of the whole MapReduce job
    reduceTaskNumber int, // which reduce task this is
    nMap int, // the number of map tasks that were run ("M" in the paper)
    reduceF func(key string, values []string) string,
) {
    // TODO:
    // You will need to write this function.
    // You can find the intermediate file for this reduce task from map task number
    // m using reduceName(jobName, m, reduceTaskNumber).
    // Remember that you've encoded the values in the intermediate files, so you
    // will need to decode them. If you chose to use JSON, you can read out
    // multiple decoded values by creating a decoder, and then repeatedly calling
    // .Decode() on it until Decode() returns an error.
    //
    // You should write the reduced output in as JSON encoded KeyValue
    // objects to a file named mergeName(jobName, reduceTaskNumber). We require
    // you to use JSON here because that is what the merger than combines the
    // output from all the reduce tasks expects. There is nothing "special" about
    // JSON -- it is just the marshalling format we chose to use. It will look
    // something like this:
    //
    // enc := json.NewEncoder(mergeFile)
    // for key in ... {
    //  enc.Encode(KeyValue{key, reduceF(...)})
    // }
    // file.Close()
    //
    // Use checkError to handle errors.

    debug("Entering doReduce function\n")
    
    var keyValuePair []KeyValue
    var kvArr_tmp []KeyValue

    for m := 0; m < nMap; m++ {
        kvArr_tmp = nil

        // Get marshalled json object produced by map function
        content, err := ioutil.ReadFile(reduceName(jobName, m, reduceTaskNumber))
        checkError(err)


        // Unmarshall json object and check for errors
        err = json.Unmarshal(content, &kvArr_tmp)
        checkError(err)

        // Merge all contents from all intermediate files 
        keyValuePair = append(keyValuePair, kvArr_tmp...)
    }
      
    // Then we need to sort the intermediate key/value pairs by key 
    sortKeyValues(keyValuePair)
    debug("doReduce - sorted keys\n %v\n", keyValuePair)

    // Setting up a map to easily keep track of unique keys and values
    keyValueMap := make(map[string][]string)

    /* Iterate through key/value pairs and using the map
     * merge all values for one same key together
     */
    for k := 0; k < len(keyValuePair); k++ {
        key := keyValuePair[k].Key
        keyValueMap[key] = append(keyValueMap[key], keyValuePair[k].Value)
    }
    
    debug("doReduce -  key/value pairs sorted and merged together\n %v \n", keyValueMap)

    // Setup output file w/encoder
    output_file, err := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_RDWR|os.O_CREATE, 0755)
    checkError(err)

    enc := json.NewEncoder(output_file)

    debug("doReduce - Output file\n")

    /* Iterate through key/value pairs, apply reduce funtion
     * and directly encode it to output file
     */
    for key, value := range keyValueMap{
        enc.Encode(KeyValue{key, reduceF(key, value)})
    }

    output_file.Close()
}

/* Helper function to sort a list of key/values by increasing key value.
 */
func sortKeyValues(kv []KeyValue) {
    sort.Slice(kv, func(i, j int) bool {
        kv1 := kv[i]
        kv2 := kv[j]
        
        compRes := strings.Compare(kv1.Key, kv2.Key)
        if (compRes == 0) {
          return false
        }
        
        return compRes > 0
    })
}

