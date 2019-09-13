package mapreduce

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
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	/* Loop over n tasks as each task need to be assigned */
	i := 0
	for i < ntasks {
		/* register channel is updated regularly with available workers */
		/* These workers need to be registered w/master */
		workerId := <-mr.registerChannel

		/* set up arguments needed for RPC call for map/reduce common tasks */
		var callArgs DoTaskArgs
		callArgs.Phase 			= phase
		callArgs.TaskNumber 	= i
		callArgs.NumOtherPhase 	= nios
		callArgs.JobName 		= mr.jobName

		/* Only the map phase needs the input files */
		if phase == mapPhase {
			callArgs.File = mr.files[i]
		}

		/* If there is an error in RPC call, status will be false */
		status := call(workerId, "Worker.DoTask", &callArgs, new(struct{}))
		if !status{
			/*TODO what happens when this DoTask RPC fails */
		}

		var registrationArgs RegisterArgs
		registrationArgs.Worker = workerId
		registrationStatus := call("Mayra", "Master.Register", registrationArgs, new(struct{}))
		if !registrationStatus{
			/*TODO what happens when registrations RPC fails */
		}

		i++
	}

	debug("Schedule: %v phase done\n", phase)
}
