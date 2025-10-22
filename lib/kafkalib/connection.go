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
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/twmb/franz-go/pkg/kgo"
	fgoAws "github.com/twmb/franz-go/pkg/sasl/aws"
	fgoPlain "github.com/twmb/franz-go/pkg/sasl/plain"
	fgoScram "github.com/twmb/franz-go/pkg/sasl/scram"
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
		KeepAlive: 30 * time.Second,
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
		if c.username != "" && c.password != "" {
			dialer.SASLMechanism = plain.Mechanism{
				Username: c.username,
				Password: c.password,
			}
			if !c.disableTLS {
				dialer.TLS = &tls.Config{}
			}
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
		if c.username != "" && c.password != "" {
			transport.SASL = plain.Mechanism{
				Username: c.username,
				Password: c.password,
			}
			if !c.disableTLS {
				transport.TLS = &tls.Config{}
			}
		}
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %s", c.Mechanism())
	}

	return transport, nil
}

func (c Connection) ClientOptions(ctx context.Context, brokers []string, awsOptFns ...func(options *awsCfg.LoadOptions) error) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConnIdleTimeout(c.timeout),
	}

	switch c.Mechanism() {
	case ScramSha512:
		mechanism := fgoScram.Auth{
			User: c.username,
			Pass: c.password,
		}.AsSha512Mechanism()

		opts = append(opts, kgo.SASL(mechanism))
		if !c.disableTLS {
			opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
		}
	case AwsMskIam:
		awsCfg, err := awsCfg.LoadDefaultConfig(ctx, awsOptFns...)
		if err != nil {
			return nil, fmt.Errorf("failed to load aws configuration: %w", err)
		}

		creds, err := awsCfg.Credentials.Retrieve(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve aws credentials: %w", err)
		}

		opts = append(opts, kgo.SASL(fgoAws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
		}.AsManagedStreamingIAMMechanism()))
		// AWS MSK always requires TLS
		opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
	case Plain:
		if c.username != "" && c.password != "" {
			mechanism := fgoPlain.Auth{
				User: c.username,
				Pass: c.password,
			}.AsMechanism()

			opts = append(opts, kgo.SASL(mechanism))
			if !c.disableTLS {
				opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
			}
		} else if !c.disableTLS {
			// No SASL mechanism, but may still need TLS
			opts = append(opts, kgo.Dialer((&tls.Dialer{Config: &tls.Config{}}).DialContext))
		}
	default:
		return nil, fmt.Errorf("unsupported kafka mechanism: %q", c.Mechanism())
	}

	return opts, nil
}
