package logger

import (
	"github.com/sirupsen/logrus"
	"os"
)

func NewLogger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(os.Stdout)

	return log
}
