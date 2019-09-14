package main

import "os"
import "fmt"
import "mapreduce"
import "unicode"
import "strings"
import "strconv"
import "sort"

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	//fmt.Println("Entering mapF function")

	var keyValuePair []mapreduce.KeyValue
	
	// Set up function for FieldsFunc, breaks when it detects nonletter
	f := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	
	//FieldsFunc w/function f above returns string in words
	words := strings.FieldsFunc(value,f)

	// The intermediate key/value pair will be word,document
	for _ , token := range words {
        keyValuePair = append(keyValuePair, mapreduce.KeyValue{token, document})
    }
	
	//fmt.Println("Intermediate key/value pair\n %v\n", keyValuePair)
	
    return keyValuePair
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	//fmt.Println("Entering reduceF function")

    /* Iterate through all the array of values received
     * this way we have access to each document per key
     * Using a map to filter and keep unique documents only
     */
 	docMap := make(map[string]int)
    for _, document := range values {
		_, ok := docMap[document]
			// Only if there is no entry already, add new one
			if !ok{
				docMap[document] = 1
			} 
    }

    /* Set up a slice w/ capacity for # documents
     * Iterate through the map and append the indices
     * which correspond to document names.
     */
    documentList := make([]string, 0, len(docMap))
    for document, _ := range docMap{
		documentList = append(documentList, document)		
	}

	// Sort list of documents
	sort.Strings(documentList)

	// Result is the # of documents followed by comma separated list of documents
	result := strconv.Itoa(len(documentList)) + " " + strings.Join(documentList, ",")

	return result
}


// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
