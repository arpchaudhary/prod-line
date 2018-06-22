package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	HEARTBEAT_PULSE_TIME = time.Second
)

type Dispatcher struct {
	config      DispatcherConfig
	workerPool  chan chan Job
	jobQueue    chan Job
	workerWg    *sync.WaitGroup
	dispWg      *sync.WaitGroup
	pulseTicker *time.Ticker
	workerMap   map[int]*Worker
	stats       DispatcherStats
}

type DispatcherStats struct {
	counter      int
	execTime     time.Duration
	poolTime     time.Duration
	jobsFailed   int
	jobsSuccess  int
	jobsEnqueued int
	throughput   int
}

type DispatcherConfig struct {
	Name         string
	JobQueueSize int
	MaxWorkers   int
	WorkerBurst  int
}

func (config *DispatcherConfig) validate() error {
	if config.JobQueueSize < 1 {
		return fmt.Errorf("Invalid JobQueueSize %d for dispatcher named %s", config.JobQueueSize, config.Name)
	}

	if config.WorkerPoolSize < 1 {
		return fmt.Errorf("Invalid worker pool size %d for dispatcher named %s", config.WorkerPoolSize, config.Name)
	}

	if config.WorkerBurst < 1 {
		return fmt.Errorf("Invalid worker burst mode size %d for dispatcher named %s", config.WorkerBurst, config.Name)
	}
	return nil
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		id := i + 1
		d.workerWg.Add(1)
		worker := NewWorker(id, d.workerPool, d.workerWg)
		d.workerMap[id] = worker
		worker.Start()
	}
	go d.heartbeat()
	go d.dispatch()

}

func (d *Dispatcher) heartbeat() {

	//This should update the status of the dispatchers for logging and
	//metric calculation purposes
	for range d.pulseTicker.C {
		//Get the status from all workers
		//Print the number of workers currently alive
		//Print the number of jobs executed and succesful
		//Print the amount of time spent idle
		//Print the amount of time spent in pool
		aliveCount := 0
		d.stats.counter += 1
		throughput := 0
		runCount := 0
		for _, worker := range d.workerMap {
			runCount += 1
			workerStats := worker.Stats()
			if workerStats.alive {
				aliveCount += 1
			}
			d.stats.jobsFailed += workerStats.jobsFailed
			d.stats.jobsSuccess += workerStats.jobsSuccess

			//Calculate a rolling average
			//throughput := ((throughput * (runCount-1)) + workerStats.throughput)/runCount
		}
		d.stats.throughput = throughput
		log.Println("[HBeat] Alive:", aliveCount, " Success:", d.stats.jobsSuccess, " Failed:", d.stats.jobsFailed)

	}

}

func (d *Dispatcher) dispatch() {
	d.dispWg.Add(1)

	for job := range d.jobQueue {

		//fmt.Printf("fetching workerJobQueue for: %s\n", job.Name())
		workerJobQueue := <-d.workerPool
		//fmt.Printf("Adding %s to workerJobQueue\n", job.ID())
		workerJobQueue <- job
	}
	log.Printf("Dispatcher jobQueue has ended.\n")

	//Close all the workers that are entering the queue now
	for worker := range d.workerPool {
		close(worker)
	}

	//Need to shut down the workerPool
	log.Printf("Worker Pool closed\n")
	d.dispWg.Done()

	//We have closed the input for the workers. Waiting for them to shutdown now

}

func (d *Dispatcher) Add(job Job) {
	d.jobQueue <- job
}

func (d *Dispatcher) Close() {
	// No more Adding jobs to the jobqueue function
	close(d.jobQueue)

	//Wait for the workers to close
	d.workerWg.Wait()

	//Close down the worker pool
	close(d.workerPool)
	d.dispWg.Wait()

	d.pulseTicker.Stop()

	//release referencers to the workers created
	d.workerMap = nil
	log.Println("Dispatcher closed")

}

// NewDispatcher creates, and returns a new Dispatcher object.
func NewDispatcher(config DispatcherConfig) (*Dispatcher, error) {

	if config == nil {
		return nil, fmt.Errorf("Dispatcher cannot be started with a nil config")
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Dispatcher{
		config: config,
		jobQueue:    make(chan Job, config.JobQueueSize),
		maxWorkers:  config.MaxWorkers,
		workerPool:  make(chan chan Job, config.MaxWorkers),
		workerWg:    &sync.WaitGroup{},
		dispWg:      &sync.WaitGroup{},
		workerMap:   make(map[int]*Worker),
		stats:       DispatcherStats{},
		pulseTicker: time.NewTicker(HEARTBEAT_PULSE_TIME),
	}, nil
}
