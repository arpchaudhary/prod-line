package main

import (
	"fmt"
	"sync"
)



type Dispatcher struct {
	name       string
	workerPool chan chan Job
	maxWorkers int
	jobQueue   chan Job
	workerWg   *sync.WaitGroup
	dispWg 	   *sync.WaitGroup
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		id := i + 1
		d.workerWg.Add(1)
		worker := NewWorker(id, d.workerPool, d.workerWg)
		worker.start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	d.dispWg.Add(1)

	for job := range d.jobQueue{
		
		//fmt.Printf("fetching workerJobQueue for: %s\n", job.Name())
		workerJobQueue := <-d.workerPool
		//fmt.Printf("Adding %s to workerJobQueue\n", job.ID())
		workerJobQueue <- job
	}
	fmt.Printf("Dispatcher jobQueue has ended.\n")

	//Close all the workers that are entering the queue now
	for worker := range d.workerPool {
		close(worker)
	}
	
	//Need to shut down the workerPool
	fmt.Printf("Worker Pool closed\n")
	d.dispWg.Done()

	//We have closed the input for the workers. Waiting for them to shutdown now
	
}

func (d *Dispatcher) AddJob(job Job) {
	d.jobQueue <- job
}

func (d *Dispatcher) SafeClose() {
	// No more Adding jobs to the jobqueue function
	close(d.jobQueue)
	
	d.workerWg.Wait()
	close(d.workerPool)
	d.dispWg.Wait()
	fmt.Println("Dispatcher closed")
	//TODO: Need to wait here for a signal from the dispatcher that it has exited

}

// NewDispatcher creates, and returns a new Dispatcher object.
func NewDispatcher(name string, maxWorkers int) *Dispatcher {
	workerPool := make(chan chan Job, maxWorkers)
	jobQueue := make(chan Job, DefaultChannelSize)

	return &Dispatcher{
		name:       name,
		jobQueue:   jobQueue,
		maxWorkers: maxWorkers,
		workerPool: workerPool,
		workerWg:   &sync.WaitGroup{},
		dispWg:     &sync.WaitGroup{},
	}
}