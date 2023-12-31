package main

import (
	"fmt"
	"os"
	"sort"
	"src/mapreduce"
	"strconv"
	"strings"
	"unicode"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you should complete this to do the inverted index challenge
	split_fun := func(r rune) bool { return !unicode.IsLetter(r) }

	kva := []mapreduce.KeyValue{}
	for _, w := range strings.FieldsFunc(value, split_fun) {
		kv := mapreduce.KeyValue{w, document}
		kva = append(kva, kv)
	}
	return kva
}

type ByKey []string

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i] < a[j] }

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you should complete this to do the inverted index challenge
	seen := make(map[string]bool)
	dedup_values := []string{}

	for _, w := range values {
		if !seen[w] {
			dedup_values = append(dedup_values, w)
			seen[w] = true
		}
	}

	sort.Sort(ByKey(dedup_values))

	var sb strings.Builder
	sb.WriteString(strconv.Itoa(len(dedup_values)))
	sb.WriteString(" ")

	for i := 0; i < len(dedup_values); i++ {
		sb.WriteString(dedup_values[i])

		endStr := ","
		if i == len(dedup_values)-1 {
			endStr = ""
		}
		sb.WriteString(endStr)
	}
	return sb.String()
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
