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

	/* Loop over n tasks as each task need to be assigned */
	var wait_group sync.WaitGroup
	// firstly, we have nTasks, and our job is dividing these tasks into the worker by the call function
	for i:=0; i < ntasks; i++ {
	    wait_group.Add(1)

	    //struct of DoTaskArgs: the information of the job
	    var args DoTaskArgs
	    args.JobName = mr.jobName
	    args.File = mr.files[i]
	    args.Phase = phase
	    args.TaskNumber = i
	    args.NumOtherPhase = nios
	    
	    // use go routines
	    go func ()  {
	        defer wait_group.Done()
	        // keep runing until success
	        for {
	            // all the worker is stored in the registerChan channel
	            worker := <-mr.registerChannel

	            if (call(worker, "Worker.DoTask", &args, nil)){
	                go func(){mr.registerChannel <- worker} ()
	                break
	            }
	        }
	    }()
	}
	// the finish
	wait_group.Wait()

	debug("Schedule: %v phase done\n", phase)
}
