package main

import (
	"fmt"
	"sync"
)

type NodeVal int

type Node struct {
	val   NodeVal
	left  *Node
	right *Node
}

func (node *Node) sum_sub() NodeVal {
	if node == nil {
		return 0
	}
	return node.val + node.left.sum_sub() + node.right.sum_sub()
}

func build_fake_tree() *Node {
	root := &Node{val: 1}
	root.left = &Node{val: 2}
	root.right = &Node{val: 3}
	root.left.left = &Node{val: 4}

	return root
}

func medium_6() {
	tree_root := build_fake_tree()
	fmt.Println("sums all the numbers in the tree:", tree_root.sum_sub())
}

// 原始位置 并发square更新
func square_conc(sli []int) {
	var wg sync.WaitGroup

	n := len(sli)

	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			sli[id] = sli[id] * sli[id]

		}(i)
	}

	wg.Wait()
}

func medium_7() {
	sli := []int{1, 2, 3, 4, 5}
	fmt.Println("origin sli:", sli)
	square_conc(sli)
	fmt.Println("after op, sli:", sli)

}

func main() {

	medium_6()
	medium_7()

}
