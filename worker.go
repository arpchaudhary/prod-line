package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	DefaultChannelSize = 100
)

type Worker struct {
	id         int
	jobQueue   chan Job
	workerPool chan chan Job
	quitChan   chan bool
	wg         *sync.WaitGroup
	execTime   time.Duration
	poolTime   time.Duration
}

func (w *Worker) start() {

	go func() {

		defer func() {
			fmt.Printf("Worker[%d] spent %s time processing and %s time in pool\n", w.id, w.execTime, w.poolTime)
			w.wg.Done()
			
			//fmt.Printf("Worker[%d] has finished\n", w.id)
        	// recover from panic if one occured. Set err to nil otherwise.
        	if (recover() != nil) {
        		//fmt.Printf("Worker[%d] has hit panic. Recovered\n", w.id)
        		//silent death
        	}
    	}()

    	fmt.Printf("Worker[%d] is alive!\n", w.id)
    	
		//Mark attendance in the worker pool
		w.workerPool <- w.jobQueue
		poolStart := time.Now()

		for job := range w.jobQueue {
			w.poolTime += time.Since(poolStart)
			// Add my jobQueue to the worker pool.
			execStart := time.Now()
			job.Execute()
			w.execTime += time.Since(execStart) 
			//fmt.Printf("Worker[%d] completed %s\n", w.id, job.ID())
			
			//Add the worker back to the pool
			//This can panic if the worker pool is already closed 
			//Shutting down state
			w.workerPool <- w.jobQueue
			poolStart = time.Now()
		}
		//The channel has closed. Time to shutdown. Stackunwind will decrement the counter
		
	}()
}



// NewWorker creates takes a numeric id and a channel w/ worker pool.
func NewWorker(id int, workerPool chan chan Job, wg *sync.WaitGroup) *Worker {
	return &Worker{
		id:         id,
		jobQueue:   make(chan Job),
		workerPool: workerPool,
		quitChan:   make(chan bool),
		wg:         wg,
	}
}

