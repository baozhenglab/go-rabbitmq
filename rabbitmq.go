package gorabbitmq

import (
	"flag"
	"fmt"
	"github.com/baozhenglab/go-sdk/v2/logger"
	"github.com/streadway/amqp"
	"sync"
)

type rabbitMQService struct {
	strConn string
	once *sync.Once
	conn *amqp.Connection
	isRunning bool
	logger logger.Logger
}

func (*rabbitMQService) Name() string {
	return  KeyService
}

func (*rabbitMQService) GetPrefix() string {
	return KeyService
}

func (rs *rabbitMQService) InitFlags(){
	prefix := fmt.Sprintf("%s-",rs.Name())
	flag.StringVar(&rs.strConn,prefix+"uri-connect","","URI connect rabbitmq")
}

func (rs *rabbitMQService) Configure() error {
	rs.logger = logger.GetCurrent().GetLogger("rabbitmq")
	return nil
}

func (rs *rabbitMQService) Run() error {
	if rs.isRunning {
		return nil
	}
	rs.Configure()
	rs.logger.Infof("conneting to rabbitmq server with uri is %s",rs.strConn)
	conn, err := amqp.Dial(rs.strConn)
	if err != nil {
		rs.logger.Errorf("Error connect to rabbitmq %v",err)
		return err
	}
	rs.logger.Info("Connect successfully to rabbitmq service")
	rs.isRunning = true
	rs.conn = conn
	go rs.reconnectIfFail()
	return nil
}

func (rs *rabbitMQService) Stop() <-chan bool {
	rs.logger.Info("Have notify close connect")
	c := make(chan bool)
	go func(){
		rs.conn.Close()
		rs.logger.Info("Closed connect to rabbitmq")
		c <- true
	}()
	return  c
}

func (rs *rabbitMQService) Get() interface{} {
	rs.once.Do(func(){
		if rs.isRunning == false {
			conn, err := amqp.Dial(rs.strConn)
			if err != nil {
				rs.logger.Errorf("Error connect to rabbitmq %v",err)
			}
			rs.logger.Info("Connect successfully to rabbitmq service")
			rs.isRunning = true
			rs.conn = conn
			go rs.reconnectIfFail()
		}
	})
	return rs.conn
}

func (rs *rabbitMQService) reconnectIfFail() {
	conn := rs.conn
	notify :=  conn.NotifyClose(make(chan *amqp.Error))
	for{
		select {
		case err := <- notify:
			if err != nil {
				rs.logger.Errorf("error connect to rabbitmq service: %v",err)
				conn.Close()
				rs.logger.Info("Need reconnect rabbitmq to service running")
				rs.isRunning = false
				rs.once = new(sync.Once)
				rs.Get()
			}
			return
		}
	}
}