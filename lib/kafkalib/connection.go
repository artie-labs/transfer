package kafkalib

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/segmentio/kafka-go/sasl/plain"
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

	// support azure event hub
	if c.username == "$ConnectionString" {
		return Plain
	}

	if c.username != "" && c.password != "" {
		return ScramSha512
	}

	return Plain
}

func (c Connection) Dialer(ctx context.Context, awsOptFns ...func(options *awsCfg.LoadOptions) error) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   c.timeout,
		DualStack: true,
	}

	switch c.Mechanism() {
	case ScramSha512:
		mechanism, err := scram.Mechanism(scram.SHA512, c.username, c.password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
		}

		dialer.SASLMechanism = mechanism
		if !c.disableTLS {
			dialer.TLS = &tls.Config{}
		}
	case AwsMskIam:
		_awsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsOptFns...)
		if err != nil {
			return nil, fmt.Errorf("failed to load aws configuration: %w", err)
		}

		dialer.SASLMechanism = aws_msk_iam_v2.NewMechanism(_awsCfg)
		// We don't need to disable TLS for AWS IAM since MSK will always enable TLS.
		dialer.TLS = &tls.Config{}
	case Plain:
		dialer.SASL = plain.Mechanism{
			Username: cfg.Kafka.Username,
			Password: cfg.Kafka.Password,
		}
		if !c.disableTLS {
			dialer.TLS = &tls.Config{}
		}
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %s", c.Mechanism())
	}

	return dialer, nil
}

func (c Connection) Transport(ctx context.Context, awsOptFns ...func(options *awsCfg.LoadOptions) error) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		DialTimeout: c.timeout,
	}

	switch c.Mechanism() {
	case ScramSha512:
		mechanism, err := scram.Mechanism(scram.SHA512, c.username, c.password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM mechanism: %w", err)
		}

		transport.SASL = mechanism
		if !c.disableTLS {
			transport.TLS = &tls.Config{}
		}
	case AwsMskIam:
		_awsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsOptFns...)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
		}

		transport.SASL = aws_msk_iam_v2.NewMechanism(_awsCfg)
		if !c.disableTLS {
			transport.TLS = &tls.Config{}
		}
	case Plain:
		dialer.SASL = plain.Mechanism{
			Username: cfg.Kafka.Username,
			Password: cfg.Kafka.Password,
		}
		if !c.disableTLS {
			dialer.TLS = &tls.Config{}
		}
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %s", c.Mechanism())
	}

	return transport, nil
}
