package mapreduce

import (
	"fmt"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var workerFree chan string = make(chan string)   // Channel used for signaling free workers
	var stopReg chan bool = make(chan bool)   // Channel used for exiting registration goroutine after all tasks have been completed
	var wg sync.WaitGroup

	// Notify that presently registered workers are free
	// Then listen for newly registered workers and notify that they are free.
	// Exit this goroutine after all tasks are completed
	go func() {
		for _, worker := range mr.workers {
			workerFree <- worker
		}

		// Goroutine will listen for any new worker registrations
		for {
			select {
			case freeWorker := <- mr.registerChannel:
				workerFree <- freeWorker
			case <-stopReg:
				return
			}
		}
	}()

	// Run tasks on free workers
	for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
		worker := <- workerFree
		args := &DoTaskArgs{mr.jobName, mr.files[taskNumber], phase, taskNumber, nios}

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Run task on a worker, if it fails then run on the next free worker until task is completed
			for {
				ok := call(worker, "Worker.DoTask", args, new(struct{}))
				if ok {
					break
				} else {
					worker = <- workerFree
				}
			}

			// If all tasks have finished by the completion of this RPC then,
			// return without notifying that the worker is free. (otherwise the channel will be full and it will block)
			if taskNumber == ntasks {
				return
			}

			// After Task is completed, worker is freed
			workerFree <- worker
		}()
	}

	// Wait for all of the tasks to finish
	wg.Wait()
	stopReg <- true
	fmt.Printf("Schedule: %v phase done\n", phase)
}
