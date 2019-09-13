
package mapreduce

import (
    "fmt"
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
    //fmt.Printf("REDUCE nMap [%d]\n", nMap)
    
    var kvArr []KeyValue
    // Call reduce function for each map
    for m := 0; m < nMap; m++ {
      // Get marshalled json object produced by map function
      content, err := ioutil.ReadFile(reduceName(jobName, m, reduceTaskNumber))
      checkError(err)

      var kvArr_tmp []KeyValue
      err = json.Unmarshal(content, &kvArr_tmp)
      checkError(err)
      
      kvArr = append(kvArr, kvArr_tmp...)
    }
      
    //fmt.Printf("REDUCE sort keys\n")
    // Then we need to sort the intermediate key/value pairs by key 
    sortKeyValues(kvArr)

    // Now, for each key, we need to generate a list of that key's string value (merged across all inputs)
    // falta esto ... para no pasar emptyString mas abajo... enc.Encode(KeyValue{kvArr[i].Key, reduceF(kvArr[i].Key, emptyString)})
    countMap := make(map[string][]string)

    for k := 0; k < len(kvArr); k++ {
        key_temp := kvArr[k].Key
        value_tem := kvArr[k].Value

        _, ok := countMap[key_temp]
        _ = ok
        countMap[key_temp] = append(countMap[key_temp], value_tem)
    }
    //fmt.Printf("REDUCE nMap [%v]\n", countMap)
    // Write result to output file
    output_file, err := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_RDWR|os.O_CREATE, 0755)
    checkError(err)

    enc := json.NewEncoder(output_file)

    fmt.Printf("REDUCE: newest logic\n")
    for keyVal, valueArr := range countMap{
        enc.Encode(KeyValue{keyVal, reduceF(keyVal, valueArr)})
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

