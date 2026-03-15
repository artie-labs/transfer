package awslib

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

type KMSClient struct {
	cfg    aws.Config
	client *kms.Client
}

func NewKMSClient(cfg aws.Config) KMSClient {
	return KMSClient{
		cfg:    cfg,
		client: kms.NewFromConfig(cfg),
	}
}

type GenerateDataKeyResult struct {
	PlaintextDEK string
	EncryptedDEK string
}

func (k KMSClient) GenerateDataKey(ctx context.Context, kmsKeyARN string) (GenerateDataKeyResult, error) {
	output, err := k.client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   aws.String(kmsKeyARN),
		KeySpec: types.DataKeySpecAes256,
	})
	if err != nil {
		return GenerateDataKeyResult{}, fmt.Errorf("failed to generate data key: %w", err)
	}

	return GenerateDataKeyResult{
		PlaintextDEK: base64.StdEncoding.EncodeToString(output.Plaintext),
		EncryptedDEK: base64.StdEncoding.EncodeToString(output.CiphertextBlob),
	}, nil
}

func (k KMSClient) DecryptDataKey(ctx context.Context, encryptedDEK string) (string, error) {
	ciphertextBlob, err := base64.StdEncoding.DecodeString(encryptedDEK)
	if err != nil {
		return "", fmt.Errorf("failed to decode encrypted DEK: %w", err)
	}

	output, err := k.client.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: ciphertextBlob})
	if err != nil {
		return "", fmt.Errorf("failed to decrypt data key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(output.Plaintext), nil
}
