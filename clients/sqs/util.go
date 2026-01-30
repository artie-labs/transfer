package sqs

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func firstError(failures []types.BatchResultErrorEntry) string {
	if len(failures) == 0 {
		return ""
	}

	for _, failure := range failures {
		if failure.Message != nil {
			return *failure.Message
		}
	}

	return ""
}
