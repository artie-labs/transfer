package s3

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type S3TestSuite struct {
	suite.Suite
}

func TestS3TestSuite(t *testing.T) {
	suite.Run(t, new(S3TestSuite))
}
