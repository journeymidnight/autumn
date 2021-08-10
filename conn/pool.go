/*
 * Copyright 2016-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package conn

import (
	"context"
	"sync"
	"time"

	"github.com/journeymidnight/autumn/proto/pb"
	"github.com/journeymidnight/autumn/utils"
	"github.com/journeymidnight/autumn/xlog"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	// ErrNoConnection indicates no connection exists to a node.
	ErrNoConnection = errors.New("No connection exists")
	// ErrUnhealthyConnection indicates the connection to a node is unhealthy.
	ErrUnhealthyConnection = errors.New("Unhealthy connection")
	//EchoDuration           = 500 * time.Millisecond
	EchoDuration = 2 * time.Second
)

// Pool is used to manage the grpc client connection(s) for communicating with other
// worker instances.  Right now it just holds one of them.
type Pool struct {
	sync.RWMutex
	// A pool now consists of one connection. gRPC uses HTTP2 transport to combine
	// messages in the same TCP stream.
	conn *grpc.ClientConn

	lastEcho time.Time
	Addr     string
	stopper  *utils.Stopper
}

// Pools manages a concurrency-safe set of Pool.
type Pools struct {
	sync.RWMutex
	all map[string]*Pool
}

var pi *Pools

func init() {
	pi = new(Pools)
	pi.all = make(map[string]*Pool)
}

// GetPools returns the list of pools.
func GetPools() *Pools {
	return pi
}

// Get returns the list for the given address.
func (p *Pools) Get(addr string) (*Pool, error) {
	p.RLock()
	defer p.RUnlock()
	pool, ok := p.all[addr]
	if !ok {
		return nil, ErrNoConnection
	}
	if !pool.IsHealthy() {
		return nil, ErrUnhealthyConnection
	}
	return pool, nil
}

func (p *Pools) remove(addr string) {
	p.Lock()
	defer p.Unlock()
	pool, ok := p.all[addr]
	if !ok {
		return
	}
	xlog.Logger.Warnf("DISCONNECTING from %s\n", addr)
	delete(p.all, addr)
	pool.shutdown()
}

func (p *Pools) getPool(addr string) (*Pool, bool) {
	p.RLock()
	defer p.RUnlock()
	existingPool, has := p.all[addr]
	return existingPool, has
}

// Connect creates a Pool instance for the node with the given address or returns the existing one.
func (p *Pools) Connect(addr string) *Pool {
	existingPool, has := p.getPool(addr)
	if has {
		return existingPool
	}

	pool, err := newPool(addr)
	if err != nil {
		xlog.Logger.Errorf("Unable to connect to host: %s", addr)
		return nil
	}

	p.Lock()
	defer p.Unlock()
	existingPool, has = p.all[addr]
	if has {
		go pool.shutdown() // Not being used, so release the resources.
		return existingPool
	}
	xlog.Logger.Infof("CONNECTING to %s\n", addr)
	p.all[addr] = pool
	return pool
}

// newPool creates a new "pool" with one gRPC connection, refcount 0.
func newPool(addr string) (*Pool, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(130<<20),
			grpc.MaxCallSendMsgSize(130<<20),
			grpc.UseCompressor((snappyCompressor{}).Name())),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	pl := &Pool{conn: conn, Addr: addr, lastEcho: time.Now(), stopper: utils.NewStopper()}
	pl.stopper.RunWorker(func() {
		pl.MonitorHealth()
	})
	return pl, nil
}

// Get returns the connection to use from the pool of connections.
func (p *Pool) Get() *grpc.ClientConn {
	p.RLock()
	defer p.RUnlock()
	return p.conn
}

func (p *Pool) shutdown() {
	xlog.Logger.Warnf("Shutting down extra connection to %s", p.Addr)
	p.stopper.Stop()
	if err := p.conn.Close(); err != nil {
		xlog.Logger.Warnf("Could not close pool connection with error: %s", err)
	}
}


func (p *Pool) LastEcho() time.Time {
	p.RLock()
	defer p.RUnlock()
	return p.lastEcho
}

// SetUnhealthy marks a pool as unhealthy.
func (p *Pool) SetUnhealthy() {
	p.Lock()
	defer p.Unlock()
	p.lastEcho = time.Time{}
}

func (p *Pool) listenToHeartbeat() error {
	conn := p.Get()
	c := pb.NewExtentServiceClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := c.Heartbeat(ctx, &pb.Payload{})
	if err != nil {
		return err
	}

	p.stopper.RunWorker(func() {
		select {
		case <-ctx.Done():
		case <-p.stopper.ShouldStop():
			cancel()
		}
	})

	// This loop can block indefinitely as long as it keeps on receiving pings back.
	for {
		_, err := s.Recv()
		if err != nil {
			return err
		}
		// We do this periodic stream receive based approach to defend against network partitions.
		p.Lock()
		p.lastEcho = time.Now()
		p.Unlock()
	}
}

// MonitorHealth monitors the health of the connection via Echo. This function blocks forever.
func (p *Pool) MonitorHealth() {
	var lastErr error
	for {
		select {
		case <-p.stopper.ShouldStop():
			return
		default:
			err := p.listenToHeartbeat()
			if lastErr != nil && err == nil {
				xlog.Logger.Infof("Connection established with %v\n", p.Addr)
			} else if err != nil && lastErr == nil {
				xlog.Logger.Warnf("Connection lost with %v. Error: %v\n", p.Addr, err)
			}
			lastErr = err
			// Sleep for a bit before retrying.
			time.Sleep(EchoDuration)
		}
	}
}

// IsHealthy returns whether the pool is healthy.
func (p *Pool) IsHealthy() bool {
	if p == nil {
		return false
	}
	p.RLock()
	defer p.RUnlock()
	return time.Since(p.lastEcho) < 4*EchoDuration
}
