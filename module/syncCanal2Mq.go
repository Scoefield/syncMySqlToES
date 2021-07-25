package module

import (
	"encoding/json"
	"fmt"
	"github/guanhg/syncDB-search/cache"
	"github/guanhg/syncDB-search/errorlog"
	"log"
	"runtime"
	"time"

	"github.com/streadway/amqp"
)

func SyncCanal2Mq(regex string) {
	defer func() {
		if e:=recover(); e!=nil{
			buf := make([]byte, 1<<16)
			runtime.Stack(buf, true)
			log.Printf("[Publishing Error] Routine: %v, stack buf: %v\n", e, string(buf))
		}
	}()

	canal := cache.GetDefaultCanal()
	rq := cache.NewMqContext()
	log.Printf("=======[Start Sync DB]======\n")
	rqDeadOptions := cache.MqOptions{Exchange: "db_sync", ExchangeType: "topic", RouteKey: "db_index_dlx", Queue: "syncIndexDlx"}

	rqOptions := cache.MqOptions{Exchange: "db_sync", ExchangeType: "topic", RouteKey: "db_index", Queue: "syncIndex"}
	// 设置死信队列，在消息消费时出错会进入死信队列'rqDeadOptions'
	rqOptions.QueueArgs = map[string]interface{}{"x-dead-letter-exchange": rqDeadOptions.Exchange, "x-dead-letter-routing-key": rqDeadOptions.RouteKey}

	rq.DeclareExchangeQueue(rqOptions)
	rq.DeclareExchangeQueue(rqDeadOptions)

	for  {
		rows := canal.Get(regex, 100)
		fmt.Println("********* canal.Get len(rows)=", len(rows))
		if len(rows) <= 0 {
			time.Sleep(3 * time.Second)
			continue
		}
		for i :=range rows{  // 发送到mq
			fmt.Println("************* 发送到mq: ", rows[i])
			body, err := json.Marshal(rows[i])
			errorlog.CheckErr(err)
			err = rq.Publish(rqOptions.Exchange, rqOptions.RouteKey, false, false, amqp.Publishing{Body: body})
			errorlog.CheckErr(err)
		}
	}
}
