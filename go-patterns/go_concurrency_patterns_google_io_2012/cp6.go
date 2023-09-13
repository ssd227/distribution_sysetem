// generator: function that returns a channel

// 这个例子和cp5的效果完全一样，只是把channel给放到了子函数boring内

package main

import (
	"fmt"
	"math/rand"
	"time"
)

func boring(msg string) <-chan string {
	c := make(chan string)

	// 由于channel的阻塞性质， 必须新开一个go routuine 才能在backside塞数据
	// 同时继续执行本函数的主流程，把channel返回出去，提供channel的接受端操作
	go func() {
		for i := 0; ; i++ {
			c <- fmt.Sprintf("%s %d", msg, i)
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		}
	}()

	return c
}

func main() {
	c := boring("shao guodong") // Function returning a channel.
	for i := 0; i < 5; i++ {
		fmt.Printf("You say: %q\n", <-c)
	}

	fmt.Println("You're boring; I'm leaving.")
}
