package main

/*
精简merge sort的代码，降低滥用的并发度
*/

import (
	"fmt"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

// merge sort，使用goroutinues 和 channel 提高并发度
func merge_sort(sli []float64) []float64 {
	N := len(sli)

	if N <= 1 {
		return sli
	}

	// STEP1: sort sub_slice
	res_chan := make(chan []float64)
	split_p := int(N / 2)

	// sorted_left_part
	go func() {
		res_chan <- merge_sort(sli[:split_p])
	}()

	// sorted_right_part
	go func() {
		res_chan <- merge_sort(sli[split_p:])
	}()

	sorted_sli_part1 := <-res_chan
	sorted_sli_part2 := <-res_chan

	// STEP2: merge two sorted sub_slice
	DPrintf("[debug]sli-%v, sorted_sub-(%v, %v).\n",
		sli, sorted_sli_part1, sorted_sli_part2)

	var sorted_slice []float64

	i, j := 0, 0
	for i < len(sorted_sli_part1) && j < len(sorted_sli_part2) {
		if sorted_sli_part1[i] < sorted_sli_part2[j] {
			sorted_slice = append(sorted_slice, sorted_sli_part1[i])
			i += 1
		} else {
			sorted_slice = append(sorted_slice, sorted_sli_part2[j])
			j += 1
		}
	}
	for i < len(sorted_sli_part1) {
		sorted_slice = append(sorted_slice, sorted_sli_part1[i])
		i += 1
	}

	for j < len(sorted_sli_part2) {
		sorted_slice = append(sorted_slice, sorted_sli_part2[j])
		j += 1
	}

	DPrintf("[debug]sli%v, sorted_final:%v.\n",
		sli, sorted_slice)

	return sorted_slice

}

func harder9() {
	ori_slice := []float64{3.1, 3.2, 5, 2, 1, 6, 10}

	t1 := time.Now()
	res_slice := merge_sort(ori_slice)
	fmt.Println("cost time:", time.Now().Sub(t1))

	fmt.Println(res_slice)
}

func newRandSlice(n int) []float64 {
	var res_slice []float64
	for i := 0; i < n; i++ {
		res_slice = append(res_slice, rand.Float64())
	}
	return res_slice
}

func test_time() {
	N := int(1e6)
	ori_slice := newRandSlice(N)
	t1 := time.Now()
	res_slice := merge_sort(ori_slice)
	t2 := time.Now()

	fmt.Printf("slice N:%v; cost time:%v.\n",
		N, t2.Sub(t1))
	fmt.Println("sorted slice:", res_slice[:10])
}

func main() {
	// harder9()
	test_time()
	
	/**
	阿里云-服务器测试
		go 非inpalce goroutinue实现
			slice N:1e6; cost time:3.331049028s.

	个人pc测试
	 	docker-go
			slice N:1e6; cost time:745.07ms.
			slice N:1e7; cost time:7.47s.
	
		win-go 非inpalce goroutinue实现
	 		slice N:1e6; cost time:1.520s.
			slice N:1e7; cost time:7.88s.

		python 非inpalce同理实现
			slice N:1e6; cost time:4.4530224s.
			slice N:1e7; cost time:62.417171s.

		python 最优实现
			slice N:1e6; cost time:3.57299s.
			slice N:1e7; cost time:45.0050s.

	对比结论：
		数据量越大越能看出差距 1e7 go vs python在八倍的差距
	
	问题：
		新版本的go编译器，还是底层linux的原因。1e6快这么多，1e7算法效率又下来了。
	*/
}
