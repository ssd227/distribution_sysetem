// daisy-chain

// 后台虚空的go routuine， 建好了但是全部阻塞
// 每个go routuine 有两个channel 连接着左边和右边
// 往最右边的的 左channel里塞一个值
// 所有go routuine被激活，并连续传递+1的值
// 最后从最左边的go routuine里用channel把值读出来

package main

import (
	"fmt"
)

func f(left, right chan int) {
	left <- 1 + <-right
}

func main() {
	const n = 100000
	leftmost := make(chan int)

	left := leftmost
	right := leftmost

	for i := 0; i < n; i++ {
		right = make(chan int)
		go f(left, right)
		left = right
	}

	go func(c chan int) {
		c <- 1
	}(right)

	// right <- 1 等效的

	fmt.Println(<-leftmost)

}
