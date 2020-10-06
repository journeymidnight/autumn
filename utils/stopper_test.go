package utils

import (
	"fmt"
	"testing"
	"time"
)

func printHello(stopper *Stopper, id int) {
	for {
		select {
		case <-stopper.ShouldStop():
			fmt.Printf("close %d goroutine\n", id)
			return
		case <-time.After(time.Second):
			fmt.Printf("hello %d\n", id)
		}
	}
}
func TestStop(t *testing.T) {
	stopper := NewStopper()
	stopper.RunWorker(func() {
		printHello(stopper, 1)
	})
	stopper.RunWorker(func() {
		printHello(stopper, 2)
	})
	time.Sleep(3 * time.Second)
	stopper.Stop()

}
