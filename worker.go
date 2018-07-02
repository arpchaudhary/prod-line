package main

import (
	//"fmt"
	"log"
	"sync"
	"time"
)

const (
	DefaultChannelSize = 10000
)

type Worker struct {
	id         int
	jobQueue   chan Job
	workerPool chan chan Job
	quitChan   chan bool
	wg         *sync.WaitGroup
	burst      int
	stats      WorkerStat
}

//TODO: Some of the statistics here should be maintened for the last n seconds
//And not for the lifetime
type WorkerStat struct {
	alive       bool
	startTime   time.Time
	execTime    time.Duration
	poolTime    time.Duration
	jobsFailed  int
	jobsSuccess int
	jobsTotal   int
	throughput  int
}

func (w *Worker) Start() {
	w.stats.alive = true
	w.stats.startTime = time.Now()
	go func() {

		defer func() {
			//fmt.Printf("Worker[%d] spent %s time processing and %s time in pool\n", w.id, w.execTime, w.poolTime)
			w.stats.alive = false
			w.wg.Done()

			//fmt.Printf("Worker[%d] has finished\n", w.id)
			// recover from panic if one occured. Set err to nil otherwise.
			if recover() != nil {
				//fmt.Printf("Worker[%d] has hit panic. Recovered\n", w.id)
				//silent death
			}
		}()

		//Mark attendance in the worker pool
		w.workerPool <- w.jobQueue
		poolStart := time.Now()

		for job := range w.jobQueue {
			w.stats.poolTime += time.Since(poolStart)
			// Add my jobQueue to the worker pool.
			execStart := time.Now()
			err := job.Execute()
			log.Printf("Worker[%d] executed Job[%s]\n", w.id, job.ID())
			w.stats.execTime += time.Since(execStart)
			w.stats.jobsTotal += 1
			if err == nil {
				w.stats.jobsSuccess += 1
			} else {
				w.stats.jobsFailed += 1
			}

			//TODO: Add a throughput mechanism here

			//Add the worker back to the pool
			//This can panic if the worker pool is already closed
			//Shutting down state
			w.workerPool <- w.jobQueue
			poolStart = time.Now()
		}
		//The channel has closed. Time to shutdown. Stackunwind will decrement the counter

	}()
}

func (w *Worker) Stats() WorkerStat {
	return w.stats
}

// NewWorker creates takes a numeric id and a channel w/ worker pool.
func NewWorker(id int, workerPool chan chan Job, wg *sync.WaitGroup, burst int) *Worker {
	return &Worker{
		id:         id,
		jobQueue:   make(chan Job, burst),
		workerPool: workerPool,
		quitChan:   make(chan bool),
		wg:         wg,
		stats:      WorkerStat{},
	}
}
