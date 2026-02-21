package apachelivy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClientPool_SingleSession(t *testing.T) {
	pool := NewClientPool("http://localhost:8998", nil, nil, 0, "", "", "test-session", 0)
	assert.Equal(t, 1, len(pool.clients))
	assert.Equal(t, "test-session", pool.clients[0].sessionName)
}

func TestNewClientPool_SingleSessionExplicit(t *testing.T) {
	pool := NewClientPool("http://localhost:8998", nil, nil, 0, "", "", "test-session", 1)
	assert.Equal(t, 1, len(pool.clients))
	assert.Equal(t, "test-session", pool.clients[0].sessionName)
}

func TestNewClientPool_MultipleSessions(t *testing.T) {
	pool := NewClientPool("http://localhost:8998", nil, nil, 0, "", "", "test-session", 3)
	assert.Equal(t, 3, len(pool.clients))
	assert.Equal(t, "test-session-0", pool.clients[0].sessionName)
	assert.Equal(t, "test-session-1", pool.clients[1].sessionName)
	assert.Equal(t, "test-session-2", pool.clients[2].sessionName)
}

func TestClientPool_Next_SingleClient(t *testing.T) {
	pool := NewClientPool("http://localhost:8998", nil, nil, 0, "", "", "test-session", 1)
	for i := 0; i < 5; i++ {
		assert.Same(t, pool.clients[0], pool.Next())
	}
}

func TestClientPool_Next_RoundRobin(t *testing.T) {
	pool := NewClientPool("http://localhost:8998", nil, nil, 0, "", "", "test-session", 3)

	// The starting offset is randomized, so get the first client to determine where we are.
	first := pool.Next()
	var startIdx int
	for i, c := range pool.clients {
		if c == first {
			startIdx = i
			break
		}
	}

	// Subsequent calls should cycle through clients in order.
	for i := 1; i < 9; i++ {
		expected := pool.clients[(startIdx+i)%3]
		assert.Same(t, expected, pool.Next())
	}
}
