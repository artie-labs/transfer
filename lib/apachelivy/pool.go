package apachelivy

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
)

type ClientPool struct {
	clients []*Client
	counter atomic.Uint64
}

func NewClientPool(url string, config map[string]any, jars []string, heartbeatTimeoutInSecond int, driverMemory, executorMemory, sessionName string, numberOfSessions int) *ClientPool {
	if numberOfSessions <= 1 {
		numberOfSessions = 1
	}

	clients := make([]*Client, numberOfSessions)
	for i := range clients {
		name := sessionName
		if numberOfSessions > 1 {
			name = fmt.Sprintf("%s-%d", sessionName, i)
		}
		clients[i] = NewClient(url, config, jars, heartbeatTimeoutInSecond, driverMemory, executorMemory, name)
	}

	pool := &ClientPool{clients: clients}
	if numberOfSessions > 1 {
		// Randomize the starting offset so that multiple pods don't all round-robin in lockstep.
		pool.counter.Store(uint64(rand.IntN(numberOfSessions)))
	}
	return pool
}

func (c *ClientPool) Next() *Client {
	if len(c.clients) == 1 {
		return c.clients[0]
	}

	idx := c.counter.Add(1) - 1
	return c.clients[idx%uint64(len(c.clients))]
}
