package main

import "fmt"

type Entry struct {
	term    int
	command interface{}
}

func main() {
	log := []Entry{Entry{}}
	fmt.Println("len of log", len(log))
	log = append(log, Entry{})

	fmt.Println(log)

	for i, v := range log {
		fmt.Println(i, v)
	}

	log = append(log, Entry{term: 1, command: 3})
	fmt.Println("len of log", len(log))
	fmt.Println(log)
}
