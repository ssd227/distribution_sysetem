// Restoreing sequencing

// 相当于两个worker在干活，每次干完一轮加一个锁
// 外层的master在协调时，每次循环给每个worker一个解锁条件

package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Message struct {
	str  string
	wait chan bool
}

func boring(str string) <-chan Message {
	waitForIt := make(chan bool)
	c := make(chan Message)
	go func() {
		for i := 0; ; i++ {
			c <- Message{fmt.Sprintf("%s %d", str, i), waitForIt}
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)

			// 相当于两个worker在干活，每次干完一轮加一个锁
			<-waitForIt
		}
	}()

	return c
}

func fanIn(input1, input2 <-chan Message) <-chan Message {
	c := make(chan Message)
	go func() {
		for {
			c <- <-input1
		}
	}()
	go func() {
		for {
			c <- <-input2
		}
	}()
	return c
}

func main() {

	c := fanIn(boring("joe"), boring("ann"))

	for i := 0; i < 20; i++ {
		msg1 := <-c
		fmt.Println(msg1.str)
		msg2 := <-c
		fmt.Println(msg2.str)

		// 外层的master在协调时，每次循环给每个worker一个解锁条件
		msg1.wait <- true //这两个channel的锁是不一样的
		msg2.wait <- true // 但是这么做和cp7在效率上又变得一样了，代码逻辑还整复杂了

		fmt.Println(msg1.wait)
		fmt.Println(msg2.wait)
	}

	fmt.Println("you're boring; I'm leaving.")
}
