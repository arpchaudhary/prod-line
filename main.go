package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	//_ "worker-queue/dispatcher"
)

// func NewWork(name string, delay time.Duration) *Work {
// 	return &Work{name, delay}
// }

// Job holds the attributes needed to perform unit of work.
type Work struct {
	Name  string
	Delay time.Duration
}

func (w *Work) Execute() error {
	time.Sleep(w.Delay)
	return nil
}

func (w *Work) ID() string {
	return w.Name
}

func generateWork(workers int, term chan os.Signal, done chan bool) {

	go func() {

		//d := DefaultDispatcher("Chief")

		config := DispatcherConfig{
			Name:         "RedChief",
			JobQueueSize: 1,
			MaxWorkers:   2,
			WorkerBurst:  2,
		}

		d, _ := NewDispatcher(config)

		defer func() {
			d.Close()
			done <- true
		}()

		d.Run()
		jobCounter := 0
		delay := 1000 * time.Millisecond
		for {
			select {
			case <-term:
				//terminate on the receipt of this signal
				log.Println("Received the termination signal")
				return
			default:
				jobCounter += 1
				d.Add(&Work{
					Name:  strconv.Itoa(jobCounter),
					Delay: delay})
			}
		}

	}()
}

func main() {

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	generateWork(100, sigs, done)
	<-done

	fmt.Println("All work is done. World peace achieved")
}
