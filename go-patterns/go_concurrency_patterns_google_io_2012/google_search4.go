// avoid timeout
// 通过增加备份服务（replicate the servers），一个query 执行多个重复的查询，只取第一个最快的结果

package main

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	web   = fakeSearch("web")
	Image = fakeSearch("image")
	Video = fakeSearch("video")
)

type Result string

type Search func(query string) Result

func fakeSearch(kind string) Search {
	return func(query string) Result {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		return Result(fmt.Sprintf("%s result for %q\n", kind, query))
	}
}

func First(query string, replicas ...Search) Result {
	c := make(chan Result)
	searchReplica := func(i int) {
		c <- replicas[i](query)
	}
	// backside 同时开多个go routine 往channel里塞数据，
	// 主逻辑返回第一个就行
	for i := range replicas {
		go searchReplica(i)
	}
	return <-c
}

func Google(query string) (results []Result) {
	c := make(chan Result)
	go func() {
		c <- web(query)
	}()

	go func() {
		c <- Image(query)
	}()

	go func() {
		c <- Video(query)
	}()

	timeout := time.After(80 * time.Millisecond)

	for i := 0; i < 3; i++ {
		select {
		case result := <-c:
			results = append(results, result)
		case <-timeout:
			fmt.Println("time out(80ms)")
			return
		}
	}

	return
}

func main() {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()

	results := First("golang",
		fakeSearch("replica 1"),
		fakeSearch("replica 2"),
	)

	elapsed := time.Since(start)
	fmt.Println(results)
	fmt.Println(elapsed)

}
