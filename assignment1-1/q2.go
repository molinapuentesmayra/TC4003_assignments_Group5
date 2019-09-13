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
	sum :=0                                                                         
	for  v := range nums {
		sum += v                                                                      
	}                                                                               
	out <- sum                                                                      
	close(out)    
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	numWorkers := num    /* Number of workers to produce */
  
  file, err := os.Open(fileName)                                                  
  checkError(err)                                                                 

  intArr, err := readInts(bufio.NewReader(file))                                  
  checkError(err)                                                                 

  intArr_len := len(intArr) 

  var in_chans []chan int
  var out_chans []chan int
  
  /* Create input/output channels array */
  for i :=0; i < numWorkers; i++ {
    in_chans  = append(in_chans, make(chan int, intArr_len/numWorkers))
    out_chans = append(out_chans, make(chan int)) 
  }
  
  /* Equally split data into input channels */
  for i:=0; i < intArr_len; i= i + numWorkers {
    for chan_num := 0; chan_num < numWorkers; chan_num++ {
      if (i + chan_num < intArr_len) {
        in_chans[chan_num] <- intArr[i + chan_num]
      } else {
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
