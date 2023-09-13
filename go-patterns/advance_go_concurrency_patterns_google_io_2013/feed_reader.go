package main

import (
	"fmt"
	"math/rand"
	"time"
)

const maxPending = 10

type fetchResult struct {
	fetched []Item
	next    time.Time
	err     error
}

type Item struct {
	Title, Channel, GUID string // a subset of RSS fields
}

type Fetcher interface {
	Fetch() (item []Item, next time.Time, err error)
}

// Fetch returns a Fetcher for Items from domain.
func Fetch(domain string) Fetcher {
	return fakeFetch(domain)
}

func fakeFetch(domain string) Fetcher {
	return &fakeFetcher{channel: domain}
}

// FakeDuplicates causes the fake fetcher to return duplicate items.
var FakeDuplicates bool

func (f *fakeFetcher) Fetch() (items []Item, next time.Time, err error) {
	now := time.Now()
	next = now.Add(time.Duration(rand.Intn(5)) * 500 * time.Millisecond)
	item := Item{
		Channel: f.channel,
		Title:   fmt.Sprintf("Item %d", len(f.items)),
	}
	item.GUID = item.Channel + "/" + item.Title
	f.items = append(f.items, item)
	if FakeDuplicates {
		items = f.items
	} else {
		items = []Item{item}
	}
	return
}

type fakeFetcher struct {
	channel string
	items   []Item
}

type Subscription interface {
	Updates() <-chan Item // stream of Items
	Close() error         // shut down the stream
}

func Subscribe(fetcher Fetcher) Subscription {
	s := &sub{
		fetcher: fetcher,
		updates: make(chan Item),       // for Updates
		closing: make(chan chan error), //for Close
	}
	go s.loop()
	return s
} // converts Fetches to a stream

// sub implements the Subscription interface.
type sub struct {
	fetcher Fetcher // fetches items
	updates chan Item
	closing chan chan error
}

// loop fetches items using s.fetcher and send them
// on s.updates.  loop exits when s.Close is called

func (s *sub) loop() {
	// periodically call Fetch
	// send fetched items on the Updates channel
	// exit when Close is called, reporting any error

	var pending []Item               // appended by fetch; consumed by send
	var next time.Time               // initially January1, year 0
	var err error                    // set when Fetch fails
	var seen = make(map[string]bool) // set of item.GUIDs
	var fetchDone chan fetchResult   // if non-nil, Fetch is running

	for {
		var fetchDelay time.Duration // initally 0 (no delay)
		if now := time.Now(); next.After(now) {
			fetchDelay = next.Sub(now)
		}

		//issue：避免取太多的feed item
		var startFetch <-chan time.Time
		if fetchDone == nil && len(pending) < maxPending {
			startFetch = time.After(fetchDelay) // enable fetch case
		}

		var first Item
		var updates chan Item
		if len(pending) > 0 {
			first = pending[0]
			updates = s.updates // enable send case
		}

		select {

		case errc := <-s.closing:
			errc <- err
			close(s.updates) // tells receiver we're done
			return

		case <-startFetch:
			fetchDone = make(chan fetchResult, 1)
			go func() {
				fetched, next, err := s.fetcher.Fetch()
				fetchDone <- fetchResult{fetched, next, err}
			}()

		case result := <-fetchDone:
			fetchDone = nil

			fetched := result.fetched
			next, err = result.next, result.err

			if err != nil {
				next = time.Now().Add(10 * time.Second)
				break
			}
			for _, item := range fetched {
				if !seen[item.GUID] {
					pending = append(pending, item)
					seen[item.GUID] = true
				}
			}

		case updates <- first:
			pending = pending[1:]
		}
	}

}

func (s *sub) Updates() <-chan Item {
	return s.updates
}

func (s *sub) Close() error {
	// TODO: make loop exit
	// TODO: find out about any error
	errc := make(chan error)
	s.closing <- errc
	return <-errc
}

type merge struct {
	subs    []Subscription
	updates chan Item
	quit    chan struct{}
	errs    chan error
}

func (m *merge) Updates() <-chan Item {
	return m.updates
}

// STARTMERGECLOSE OMIT
func (m *merge) Close() (err error) {
	close(m.quit) // HL
	for _ = range m.subs {
		if e := <-m.errs; e != nil { // HL
			err = e
		}
	}
	close(m.updates) // HL
	return
}

// STARTMERGESIG OMIT
// Merge returns a Subscription that merges the item streams from subs.
// Closing the merged subscription closes subs.
func Merge(subs ...Subscription) Subscription {
	// STOPMERGESIG OMIT
	m := &merge{
		subs:    subs,
		updates: make(chan Item),
		quit:    make(chan struct{}),
		errs:    make(chan error),
	}
	// STARTMERGE OMIT
	for _, sub := range subs {
		go func(s Subscription) {
			for {
				var it Item

				select {
				case it = <-s.Updates():
				case <-m.quit: // HL
					m.errs <- s.Close() // HL
					return              // HL
				}

				select {
				case m.updates <- it:
				case <-m.quit: // HL
					m.errs <- s.Close() // HL
					return              // HL
				}
			}
		}(sub)
	}
	// STOPMERGE OMIT
	return m
}

func main() {
	// subscribe to some feeds, and create a merged update stream.

	merged := Merge(
		Subscribe(Fetch("blog.golang.org")),
		Subscribe(Fetch("googleblog.blogspot.com")),
		Subscribe(Fetch("googledevelopers.blogspot.com")))

	// Close the subscription after som time.
	time.AfterFunc(3*time.Second, func() {
		fmt.Println("closed:", merged.Close())
	})

	// Print the stream.
	for it := range merged.Updates() {
		fmt.Println(it.Channel, it.Title)
	}

	panic("show me the stacks")

}
