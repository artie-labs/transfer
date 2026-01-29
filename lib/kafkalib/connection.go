package kafkalib

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
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

func NewConnection(enableAWSMSKIAM, disableTLS bool, username, password string, timeout time.Duration) Connection {
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
