package main

import "fmt"

type A struct {
	err string
}

func main() {
	a := A{}
	if a.err == "" {
		fmt.Println("ssgd")
	}

}
