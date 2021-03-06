package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	//TODO: Currently keep at 1 second so that the stats are correct
	HEARTBEAT_PULSE_TIME = 3*time.Second
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
		return fmt.Errorf("Invalid JobQueueSize %d for %s dispatcher", config.JobQueueSize, config.Name)
	}

	if config.MaxWorkers < 1 {
		return fmt.Errorf("Invalid worker pool size %d for %s dispatcher", config.MaxWorkers, config.Name)
	}

	if config.WorkerBurst < 1 {
		return fmt.Errorf("Invalid worker burst mode size %d for %s dispatcher", config.WorkerBurst, config.Name)
	}
	return nil
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.config.MaxWorkers; i++ {
		id := i + 1
		d.workerWg.Add(1)
		worker := NewWorker(id, d.workerPool, d.workerWg, d.config.WorkerBurst)
		d.workerMap[id] = worker
		worker.Start()
	}
	log.Printf("%s dispatcher started with %d workers\n", d.config.Name, d.config.MaxWorkers)
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
		//throughput := 0
		runCount := 0
		totalJobsSuccess := 0
		totalJobsFailed := 0

		for _, worker := range d.workerMap {
			runCount += 1
			workerStats := worker.Stats()
			if workerStats.alive {
				aliveCount += 1
			}
			totalJobsFailed += workerStats.jobsFailed
			totalJobsSuccess += workerStats.jobsSuccess
		}

		newJobsProcessed := (totalJobsSuccess + totalJobsFailed) - (d.stats.jobsSuccess + d.stats.jobsFailed)
		d.stats.jobsSuccess = totalJobsSuccess
		d.stats.jobsFailed = totalJobsFailed
		//totalJobs = d.stats.jobsSuccess + d.stats.jobsFailed 
		d.stats.throughput = int(float64(newJobsProcessed) * (float64(time.Second)/float64(HEARTBEAT_PULSE_TIME)))
		log.Println("[HBeat] Alive:", aliveCount, "Success:", d.stats.jobsSuccess,
			"Fail:", d.stats.jobsFailed, "QFill:", (len(d.jobQueue)*100)/cap(d.jobQueue), `%`,
			"Output(J/s):", d.stats.throughput)
	}

}

func (d *Dispatcher) dispatch() {
	d.dispWg.Add(1)

	//Find the first worker in the pool

	workerJobQueue := <-d.workerPool
	for job := range d.jobQueue {

		select {
		case workerJobQueue <- job:
			//Pushed this job successfully
		default:
			//The particular worker queue is full
			//Find the next worker queue
			workerJobQueue = <-d.workerPool

			//Add this job now. This is a dirty hack at this point
			workerJobQueue <- job
		}

		//fmt.Printf("fetching workerJobQueue for: %s\n", job.Name())
		//fmt.Printf("Adding %s to workerJobQueue\n", job.ID())
	}

	log.Println("[Dspch]", "Job Queue allocation has stopped")

	//Close all the workers that are entering the queue now
	for worker := range d.workerPool {
		close(worker)
	}

	//Need to shut down the workerPool
	log.Printf("[Dspch] Worker Pool closed\n")
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
	log.Println("[Dspch] Shutdown complete")

}

func DefaultDispatcher(name string) *Dispatcher {
	defaultConfig := DispatcherConfig{
		Name:         name,
		MaxWorkers:   runtime.NumCPU(),
		WorkerBurst:  1,
		JobQueueSize: 1000,
	}

	d, err := NewDispatcher(defaultConfig)
	if err != nil {
		//This is the cost of using the default dispatch
		//If you dont want to take decisions, we will take them for you
		log.Fatalf("[Dspch] Dispatcher %s could not be started with defaults. Something is horribly wrong.", name)
	}
	return d
}

// NewDispatcher creates, and returns a new Dispatcher object.
func NewDispatcher(config DispatcherConfig) (*Dispatcher, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Dispatcher{
		config:      config,
		jobQueue:    make(chan Job, config.JobQueueSize),
		workerPool:  make(chan chan Job, config.MaxWorkers),
		workerWg:    &sync.WaitGroup{},
		dispWg:      &sync.WaitGroup{},
		workerMap:   make(map[int]*Worker),
		stats:       DispatcherStats{},
		pulseTicker: time.NewTicker(HEARTBEAT_PULSE_TIME),
	}, nil
}
