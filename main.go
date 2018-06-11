package main

import (
	"fmt"
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

func main() {
	delay := 10 * time.Millisecond
	allWork := []Work{
		Work{"1", delay},
		Work{"2", delay},
		Work{"3", delay},
		Work{"4", delay},
		Work{"5", delay},
		Work{"6", delay},
		Work{"7", delay},
		Work{"8", delay},
		Work{"9", delay},
		Work{"10", delay},
	}

	d := NewDispatcher("dp_1", 5)
	d.Run()

	for _, w := range allWork {
		d.AddJob(&w)
	}
	fmt.Println("Added all the work to be done")
	// time.Sleep(1 * time.Second)
	d.SafeClose()
	fmt.Println("All work is done. World peace achieved")
}