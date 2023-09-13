// timeout using select

package main

import (
	"fmt"
	"math/rand"
	"time"
)

func boring(msg string) <-chan string {
	c := make(chan string)

	go func() {
		for i := 0; ; i++ {
			c <- fmt.Sprintf("%s %d", msg, i)
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		}
	}()

	return c
}

// Timeout using select
func main() {
	c := boring("Joe")
	for {
		select {
		case s := <-c:
			fmt.Println(s)
		case s := <-time.After(time.Duration(500) * time.Millisecond): // 保证每一次service的调用时间都不超过1s
			fmt.Println("You're too slow!", s)
			return
		}
	}
}

// func main() {
// 	s := <-time.After(1 * time.Second)
// 	fmt.Println("You're too slow!", s)

// }
