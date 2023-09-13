// long-lived programs need to clean up
// how to write programs that handle communication, periodic events, and cancellation

/**

select {
case xc <- x:
	// sent x on xc
case y:= <-yc:
	// received y from yc
}

**/