package kafkalib

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const DefaultTimeout = 10 * time.Second

type Mechanism string

const (
	Plain       Mechanism = "PLAIN"
	ScramSha512 Mechanism = "SCRAM-SHA-512"
	AwsMskIam   Mechanism = "AWS-MSK-IAM"
)

type Connection struct {
	enableAWSMSKIAM bool
	disableTLS      bool
	username        string
	password        string

	timeout time.Duration
}

func NewConnection(enableAWSMSKIAM bool, disableTLS bool, username, password string, timeout time.Duration) Connection {
	return Connection{
		enableAWSMSKIAM: enableAWSMSKIAM,
		disableTLS:      disableTLS,
		username:        username,
		password:        password,
		timeout:         cmp.Or(timeout, DefaultTimeout),
	}
}

func (c Connection) Mechanism() Mechanism {
	if c.enableAWSMSKIAM {
		return AwsMskIam
	}

	if c.username != "" && c.password != "" {
		return ScramSha512
	}

	return Plain
}

// ClientOptions returns franz-go client options based on the connection configuration
func (c Connection) ClientOptions(ctx context.Context, brokers []string, awsOptFns ...func(options *awsCfg.LoadOptions) error) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConnIdleTimeout(c.timeout),
	}

	switch c.Mechanism() {
	case ScramSha512:
		mechanism := scram.Auth{
			User: c.username,
			Pass: c.password,
		}.AsSha512Mechanism()

		opts = append(opts, kgo.SASL(mechanism))
		if !c.disableTLS {
			opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
		}
	case AwsMskIam:
		_, err := awsCfg.LoadDefaultConfig(ctx, awsOptFns...)
		if err != nil {
			return nil, fmt.Errorf("failed to load aws configuration: %w", err)
		}

		mechanism := aws.Auth{
			AccessKey: "", // Will be loaded from AWS config
			SecretKey: "", // Will be loaded from AWS config
		}.AsManagedStreamingIAMMechanism()

		opts = append(opts, kgo.SASL(mechanism))
		// AWS MSK always requires TLS
		opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
	case Plain:
		// No SASL mechanism, but may still need TLS
		if !c.disableTLS {
			opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
		}
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %s", c.Mechanism())
	}

	return opts, nil
}
