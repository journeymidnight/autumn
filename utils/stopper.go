package utils

import (
	"context"
	"sync"
)

// Stopper is a manager struct for managing worker goroutines.
type Stopper struct {
	ctx context.Context
	cancel context.CancelFunc
	wg sync.WaitGroup
}

// NewStopper return a new Stopper instance.
func NewStopper(ctx context.Context) *Stopper {
	ctx, cancel := context.WithCancel(ctx)
	return &Stopper{
		ctx:    ctx,
		cancel: cancel,
	}
}

// RunWorker creates a new goroutine and invoke the f func in that new
// worker goroutine.
func (s *Stopper) RunWorker(f func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f()
	}()

}

// ShouldStop returns a chan struct{} used for indicating whether the
// Stop() function has been called on Stopper.
func (s *Stopper) ShouldStop() <- chan struct{} {
	return s.ctx.Done()
}


func (s *Stopper) Ctx() context.Context {
	return s.ctx
}

// Wait waits on the internal sync.WaitGroup. It only return when all
// managed worker goroutines are ready to return and called
// sync.WaitGroup.Done() on the internal sync.WaitGroup.
func (s *Stopper) Wait() {
	s.wg.Wait()
}

// Stop signals all managed worker goroutines to stop and wait for them
// to actually stop.
func (s *Stopper) Stop() {
	s.cancel()
	s.wg.Wait()
}

// Close closes the internal shouldStopc chan struct{} to signal all
// worker goroutines that they should stop.
func (s *Stopper) Close() {
	s.cancel()
}
