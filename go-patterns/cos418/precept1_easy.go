package main

import "fmt"

func my_suqared(n int) {
	for i := 0; i < n; i++ {
		fmt.Println(i)
	}
}

func fibonacci(n int) {
	a, b := 1, 1

	for i := 0; i < n; i++ {
		if i < 2 {
			fmt.Println("fib-", i+1, 1)
			continue
		}
		c := a + b
		fmt.Println("fib-", i+1, c)

		a, b = b, c
	}
}

// 原位置反转slice
func reverse_slice(li []int) {
	// 双指针反转,p1,p2

	for p1, p2 := 0, len(li)-1; p1 < p2; {
		li[p1], li[p2] = li[p2], li[p1]

		p1 += 1
		p2 -= 1
	}

}

func unique_n_of_slice(li []int) int {
	unique_n := 0
	seen := make(map[int]bool)

	for _, x := range li {
		if !seen[x] {
			seen[x] = true
			unique_n += 1
		}

	}
	return unique_n

}

func main() {

	// *** easy ***

	// easy-1
	// my_suqared(10)

	// easy-2
	// fibonacci(10)

	// easy-4
	sli := []int{1, 2, 3, 4, 5}
	fmt.Println("slice", sli)
	reverse_slice(sli)
	fmt.Println("reverse of slice", sli)

	// easy-5
	// li := []int{1, 2, 3, 4, 1}
	// n := unique_n_of_slice(li)
	// fmt.Println("unique num of slice-li", n)

	// *** medium ***

	// *** a little harder ***

}
