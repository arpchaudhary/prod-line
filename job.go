package main

type Job interface {
	Execute() error
	ID() string
}
