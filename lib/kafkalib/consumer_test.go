package kafkalib

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInjectFranzGoConsumerProvidersIntoContext_SingleClient(t *testing.T) {
	cfg := &Kafka{
		BootstrapServer: "localhost:9092",
		GroupID:         "test-group",
		DisableTLS:      true,
		UseSingleClient: true,
		TopicConfigs: []*TopicConfig{
			{Topic: "topic-a"},
			{Topic: "topic-b"},
			{Topic: "topic-c"},
		},
	}

	ctx, err := InjectFranzGoConsumerProvidersIntoContext(context.Background(), cfg)
	require.NoError(t, err)

	providerA, err := GetConsumerFromContext(ctx, "topic-a")
	require.NoError(t, err)
	providerB, err := GetConsumerFromContext(ctx, "topic-b")
	require.NoError(t, err)
	providerC, err := GetConsumerFromContext(ctx, "topic-c")
	require.NoError(t, err)

	// All providers should share the same underlying client.
	assert.Same(t, providerA.client, providerB.client)
	assert.Same(t, providerA.client, providerC.client)

	// Each provider should have the correct group ID.
	assert.Equal(t, "test-group", providerA.GetGroupID())
	assert.Equal(t, "test-group", providerB.GetGroupID())
	assert.Equal(t, "test-group", providerC.GetGroupID())

	providerA.client.Close()
}

func TestInjectFranzGoConsumerProvidersIntoContext_MultipleClients(t *testing.T) {
	cfg := &Kafka{
		BootstrapServer: "localhost:9092",
		GroupID:         "test-group",
		DisableTLS:      true,
		UseSingleClient: false,
		TopicConfigs: []*TopicConfig{
			{Topic: "topic-a"},
			{Topic: "topic-b"},
			{Topic: "topic-c"},
		},
	}

	ctx, err := InjectFranzGoConsumerProvidersIntoContext(context.Background(), cfg)
	require.NoError(t, err)

	providerA, err := GetConsumerFromContext(ctx, "topic-a")
	require.NoError(t, err)
	providerB, err := GetConsumerFromContext(ctx, "topic-b")
	require.NoError(t, err)
	providerC, err := GetConsumerFromContext(ctx, "topic-c")
	require.NoError(t, err)

	// Each provider should have its own client.
	assert.NotSame(t, providerA.client, providerB.client)
	assert.NotSame(t, providerA.client, providerC.client)
	assert.NotSame(t, providerB.client, providerC.client)

	// Each provider should have the correct group ID.
	assert.Equal(t, "test-group", providerA.GetGroupID())
	assert.Equal(t, "test-group", providerB.GetGroupID())
	assert.Equal(t, "test-group", providerC.GetGroupID())

	providerA.client.Close()
	providerB.client.Close()
	providerC.client.Close()
}
