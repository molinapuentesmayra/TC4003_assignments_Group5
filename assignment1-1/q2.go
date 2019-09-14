package cos418_hw1_1

import (                                                                          
        "bufio"                                                                   
        "io"                                                                      
        "strconv"
        "os"
) 

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
  // Counter to keep result
  sum :=0

  /* Iterate over all values in input channel "nums" 
   * and accumulate sum result into sum 
   */
  for  v := range nums {
    sum += v                                                                      
  }

  // Write result into output channel 
  out <- sum

  // Close output channel - no more writting operations allowed from now on 
  close(out)                                                                      
} 

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
  /* Number of workers to produce */
  numWorkers := num
  
  /* Open input file so we can start read operations */
  file, err := os.Open(fileName)
  checkError(err)

  /* Get array of integers of the provided input file */
  intArr, err := readInts(bufio.NewReader(file))                                  
  checkError(err)                                                                 

  /* Length of integer array */
  intArr_len := len(intArr) 

  /* Create arrays to hold input/output channels */
  var in_chans []chan int
  var out_chans []chan int
  
  /* Initialize input/output channels array 
   * As data will be equally splitted among input channels, we will create
   * our buffered channels of size intArr_len/numWorkers
   */
  for i :=0; i < numWorkers; i++ {
    in_chans  = append(in_chans, make(chan int, intArr_len/numWorkers))
    out_chans = append(out_chans, make(chan int)) 
  }
  
  /* Equally split data into input channels */
  for i:=0; i < intArr_len; i= i + numWorkers {
    for chan_num := 0; chan_num < numWorkers; chan_num++ {
      /* Before inserting a value into the channel, verify there is room for it */
      if (i + chan_num < intArr_len) {
        /* Insert array value into input channel */
        in_chans[chan_num] <- intArr[i + chan_num]
      } else {
        /* No room for it, so just break */
        break
      }
    }
  }                                              

  /* Lets start all the worker process */
  for chan_num := 0; chan_num < numWorkers; chan_num++ {
    /* Close input channels - we are not accepting any more write operations to input channels */
    close(in_chans[chan_num])
  
    /* Init worker process */
    go sumWorker(in_chans[chan_num], out_chans[chan_num])
  }

  retval := 0
  
  /* Lets get the result for each channel */
  for chan_num := 0; chan_num < numWorkers; chan_num++ { 
    for val := range out_chans[chan_num] {
      /* Sum up the result for each individual worker */
      retval = retval + val
    }
  }
  
  return retval                        
}   

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
  scanner := bufio.NewScanner(r)
  scanner.Split(bufio.ScanWords)
  var elems []int
  for scanner.Scan() {
    val, err := strconv.Atoi(scanner.Text())
    if err != nil {
      return elems, err
    }
    elems = append(elems, val)
  }
  return elems, nil
}
