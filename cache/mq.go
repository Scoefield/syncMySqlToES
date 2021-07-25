package cache

import (
	"fmt"
	"github/guanhg/syncDB-search/config"
	"github/guanhg/syncDB-search/errorlog"
	"log"
	"runtime"

	"github.com/streadway/amqp"
)

type MqContext struct {
	*amqp.Channel  // 结构体继承
}

func NewMqContext() *MqContext {
	ctx := new(MqContext)
	fmt.Println("amqp.Dial url: ", config.JsonConfig.AMQPUrl)
	conn, err := amqp.Dial(config.JsonConfig.AMQPUrl)
	errorlog.CheckErr(err, "could not establish connection with RabbitMQ: ")

	ch, err := conn.Channel()
	errorlog.CheckErr(err, "could not open RabbitMQ channel: ")

	ctx.Channel = ch
	return ctx
}

func (mc *MqContext)DeclareExchangeQueue(options MqOptions)  {
	err := mc.ExchangeDeclare(options.Exchange, options.ExchangeType, true, false, false, false, options.ExchangeArgs)
	errorlog.CheckErr(err)

	_, err = mc.QueueDeclare(options.Queue, true, false, false, false, options.QueueArgs)
	errorlog.CheckErr(err, "error declare queue: ")

	err = mc.QueueBind(options.Queue, options.RouteKey, options.Exchange, false, options.BindArgs)
	errorlog.CheckErr(err, "error bind queue-exchange: ")

}

// 处理消息：失败后重入死信队列
func (mc *MqContext)OnMessage(msg amqp.Delivery, handle func(msg amqp.Delivery))  {
	defer func() {
		if e:=recover(); e!=nil{
			buf := make([]byte, 1<<16)
			runtime.Stack(buf, true)
			log.Printf("[OnMessage Error] %s\n, stackBuf: %v", e, string(buf))
			mc.Nack(msg.DeliveryTag, false, false)  // nack 后会进入死信队列
		}
		mc.Ack(msg.DeliveryTag, false)
	}()
	handle(msg)
}

// -------------------------------------------------------
// MQ队列申明选项
type MqOptions struct {
	Queue string
	RouteKey string
	Exchange string
	ExchangeType string

	ExchangeArgs map[string]interface{}
	QueueArgs map[string]interface{}
	BindArgs map[string]interface{}
}

func (mo *MqOptions)SetQueue(q string) *MqOptions {
	mo.Queue = q
	return mo
}

func (mo *MqOptions)SetExchange(ex string) *MqOptions {
	mo.Exchange = ex
	return mo
}

func (mo *MqOptions)SetExchangeType(ext string) *MqOptions {
	mo.ExchangeType = ext
	return mo
}

func (mo *MqOptions)SetRouteKey(key string) *MqOptions {
	mo.RouteKey = key
	return mo
}








