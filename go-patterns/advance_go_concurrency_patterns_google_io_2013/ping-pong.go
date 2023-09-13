package main

import (
	"fmt"
	"time"
)

type Ball struct{ hits int }

func player(name string, table chan *Ball) {
	for {
		ball := <-table
		ball.hits++
		fmt.Println(name, ball.hits)
		time.Sleep(100 * time.Millisecond)
		table <- ball
	}
}

func main() {
	// table := make(chan *Ball)  version-1：deadlock for three go routinues

	table := make(chan *Ball)

	go player("A:ping", table)
	go player("B:pong", table)

	table <- new(Ball)
	time.Sleep(10 * time.Second)
	<-table

	// panic("show me the stacks") version-2：panic dumps the stacks
}
