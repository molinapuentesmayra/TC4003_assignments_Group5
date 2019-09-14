package mapreduce

import (
    "sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	
	/* A waitGroup wait for a collection of goroutines to finish
	 * Per each task, a goroutine is a added to this collection
	 * Finishes when all goroutines are done
	 */
	var wait_group sync.WaitGroup

	// Iterate over number of tasks as each task need to be assigned to a worker 
	for i:=0; i < ntasks; i++ {
	
	    // Setup doTask arguments
	    args := DoTaskArgs{
		    JobName: 		mr.jobName,
		    File: 			mr.files[i],
		    Phase: 			phase,
		    TaskNumber: 	i,
		    NumOtherPhase:	nios,
		}
	    
	    // Adding a goroutine to waitGroup
	    wait_group.Add(1)

	    go func ()  {
	        /* Infinite loop, it can break out of this loop only when worker is
	         * assigned a doTask and sent back to available registerChannel
	         */
	        for {
	            // Get an available worker from register channel
	            workerID := <-mr.registerChannel

	            // Do an RPC call for DoTask with arguments and available workerID
	            if (call(workerID, "Worker.DoTask", &args, new(struct{}))){
	            	debug("Worker succesfully assigned")

	            	// Send back worker to register channel
	                go func(){mr.registerChannel <- workerID} ()

	                /* Only if the RPC call is successfull we can break out 
	            	 * of this infinite loop. This handles worker failure or 
	            	 * bad network connections
	            	 */
	                break
	            }	        
	        }
	        // When this goroutine finishes, it can me marked as done in waitGroup
	        wait_group.Done()
	    }()
	}

	wait_group.Wait()
	debug("Schedule: %v phase done\n", phase)
}
