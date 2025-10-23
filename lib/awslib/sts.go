package awslib

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const expirationBuffer = 10 * time.Minute

type Credentials struct {
	// Generated:
	awsAccessKeyID     string
	awsSecretAccessKey string
	awsSessionToken    string
	expiresAt          time.Time
	mu                 sync.Mutex

	// Arguments to generate the credentials:
	_awsAccessKeyID     string
	_awsSecretAccessKey string
	_awsRoleARN         string
	_sessionLabel       string
}

func GenerateSTSCredentials(ctx context.Context, awsAccessKeyID, awsSecretAccessKey, roleARN, sessionLabel string) (Credentials, error) {
	creds := credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")
	cfg, err := config.LoadDefaultConfig(ctx, config.WithCredentialsProvider(creds))
	if err != nil {
		return Credentials{}, err
	}

	stsClient := sts.NewFromConfig(cfg)
	stsOutput, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         &roleARN,
		RoleSessionName: &sessionLabel,
	})
	if err != nil {
		return Credentials{}, err
	}

	return Credentials{
		awsAccessKeyID:     *stsOutput.Credentials.AccessKeyId,
		awsSecretAccessKey: *stsOutput.Credentials.SecretAccessKey,
		awsSessionToken:    *stsOutput.Credentials.SessionToken,
		expiresAt:          *stsOutput.Credentials.Expiration,

		// Metadata:
		_awsRoleARN:         roleARN,
		_sessionLabel:       sessionLabel,
		_awsAccessKeyID:     awsAccessKeyID,
		_awsSecretAccessKey: awsSecretAccessKey,
	}, nil
}

func (c *Credentials) refresh(ctx context.Context) error {
	creds, err := GenerateSTSCredentials(ctx, c._awsAccessKeyID, c._awsSecretAccessKey, c._awsRoleARN, c._sessionLabel)
	if err != nil {
		return err
	}

	c.awsAccessKeyID = creds.awsAccessKeyID
	c.awsSecretAccessKey = creds.awsSecretAccessKey
	c.awsSessionToken = creds.awsSessionToken
	c.expiresAt = creds.expiresAt
	return nil
}

func (c *Credentials) isExpired() bool {
	// 10 minute buffer
	return c.expiresAt.Before(time.Now().Add(expirationBuffer))
}

func (c *Credentials) BuildCredentials(ctx context.Context) (credentials.StaticCredentialsProvider, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isExpired() {
		if err := c.refresh(ctx); err != nil {
			return credentials.StaticCredentialsProvider{}, err
		}
	}

	return credentials.NewStaticCredentialsProvider(c.awsAccessKeyID, c.awsSecretAccessKey, c.awsSessionToken), nil
}
