package main

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

// 这个版本有点滥用goroutinue，代码复杂度太高，数值通过chan一个一个的回传，代码运行效率不见的就变高了。

// merge sort，使用goroutinues 和 channel 提高并发度
func merge_sort_conc(sli []float64, res_chan chan float64) {
	N := len(sli)

	if N == 1 {
		res_chan <- sli[0]
		DPrintf("[debug][size_1_slice] sli:%v, send:%f, and return.\n",
			sli, sli[0])

		close(res_chan)
		return // bug fix 需要跳出
	}

	split_p := int(N / 2)

	var left_chan, right_chan chan float64 // init for nil
	var left_chan_tmp, right_chan_tmp chan float64

	if len(sli[:split_p]) >= 1 {
		left_chan = make(chan float64)
		go merge_sort_conc(sli[:split_p], left_chan)
	}
	if len(sli[split_p:]) >= 1 {
		right_chan = make(chan float64)
		go merge_sort_conc(sli[split_p:], right_chan)
	}

	var left_val, right_val float64
	var left_tag, right_tag bool

	left_left := len(sli[:split_p])
	right_left := len(sli[split_p:])
	DPrintf("[debug][] sli:%v, init_left_n:(%d, %d).\n",
		sli, left_left, right_left)

	i := 0
	for {
		i += 1

		if !left_tag && left_left > 0 {
			left_chan_tmp = left_chan
		} else {
			left_chan_tmp = nil
		}

		if !right_tag && right_left > 0 {
			right_chan_tmp = right_chan
		} else {
			right_chan_tmp = nil
		}

		if left_chan_tmp != nil || right_chan_tmp != nil {
			select {
			case left_val = <-left_chan_tmp:
				left_tag = true
				left_left -= 1

			case right_val = <-right_chan_tmp:
				right_tag = true
				right_left -= 1
			}
		}

		DPrintf("[debug][each_loop] sli:%v, loop: %d,val:(%f, %f), received_n(%d, %d),.\n",
			sli, i, left_val, right_val, left_left, right_left)

		if left_tag && right_tag { // 都是新值需要比较
			DPrintf("[debug][sort_two_chan][two new] sli:%v, loop:%d val(%f, %f).\n", sli, i, left_val, right_val)
			if left_val < right_val {
				res_chan <- left_val
				left_tag = false // 旧值作废，需要通过select重新取
			} else {
				res_chan <- right_val
				right_tag = false
			}
		} else if left_tag && right_left == 0 { // 仅左channel有值
			res_chan <- left_val
			left_tag = false
			DPrintf("[debug][sort_two_chan][left new] sli:%v, loop:%d val(%f, %f).\n", sli, i, left_val, right_val)
		} else if right_tag && left_left == 0 { // 仅右channel有值
			res_chan <- right_val
			right_tag = false
			DPrintf("[debug][sort_two_chan][right new] sli:%v, loop:%d val(%f, %f).\n", sli, i, left_val, right_val)
		}

		DPrintf("[debug][sort_two_chan][loop_final_state] sli:%v, loop:%d,tag(%t, %t),left(%d, %d), break-tag:%t.\n",
			sli, i, left_tag, right_tag, left_left, right_left, (left_left+right_left) == 0 && !left_tag && !right_tag)

		if (left_left+right_left) == 0 && !left_tag && !right_tag {
			break
		}
	}

}

func merge_sort(sli []float64) []float64 {
	res_chan := make(chan float64)
	go merge_sort_conc(sli, res_chan)

	N := len(sli)

	var sorted_slice []float64

	for i := 0; i < N; i++ {
		sorted_slice = append(sorted_slice, <-res_chan)
	}
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
	N := int(1e4)
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

	// 由于数据稀碎，一个一个传回channel，速度反而慢了不少
	// 通道传数据是有开销的
	test_time()

	// slice N:1e2; cost time:872.115µs.
	// slice N:1e3; cost time:9.244953ms.
	// slice N:1e4; cost time:122.813254ms.
	// slice N:1e5; cost time:1.563431174s.

	// 比python还慢
}
