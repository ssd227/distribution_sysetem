// using channels

// channels 之间通讯的两个go routuine 是同步的(synchronization)，
// 一个塞数据、一个接受数据，都没准备好的时候逻辑就会被block(阻塞)

package main

import (
	"fmt"
	"math/rand"
	"time"
)

func boring(msg string, c chan string) {
	for i := 0; ; i++ {
		c <- fmt.Sprintf("%s %d", msg, i)
		time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
	}
}

func main() {
	c := make(chan string)
	go boring("shao guodong", c)
	for i := 0; i < 5; i++ {
		fmt.Printf("You say: %q\n", <-c)
	}

	fmt.Println("You're boring; I'm leaving.")
}
