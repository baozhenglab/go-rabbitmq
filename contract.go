package gorabbitmq

import (
	goservice "github.com/baozhenglab/go-sdk/v2"
	"sync"
)

const (
	KeyService = "rabbitmq"
)

func NewService() goservice.PrefixRunnable {
	return &rabbitMQService{
		once: new(sync.Once),
	}
}