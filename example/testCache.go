package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"github/guanhg/syncDB-search/cache"
	"github/guanhg/syncDB-search/errorlog"
	"reflect"
	"time"
)

func main() {
	testDB()
	//testRedis()

	//forever := make(chan bool)
	//go testCanal()
	//testMq()
	//<- forever
}

func testDB()  {
	ctx := cache.GetContext("default")
	defer ctx.Close()
	rows, _ := ctx.Query("select * from sm_record_2018 where id=?", 1)

	for _, row :=range rows{
		for k, v := range row{
			s := fmt.Sprintf("%20s:%20v", k, v)
			fmt.Println(s, "->", reflect.TypeOf(v))
		}
	}
}

func testCanal()  {
	canal := cache.GetDefaultCanal()
	fmt.Println("=======[Start]=====")
	rqOptions := cache.MqOptions{Exchange: "db_sync", ExchangeType: "topic", RouteKey: "statement", Queue: "sm"}
	rq := cache.NewMqContext()
	rq.DeclareExchangeQueue(rqOptions)

	for  {
		rows := canal.Get(".*\\..*", 2)
		if len(rows) <= 0 {
			time.Sleep(1 * time.Second)
			continue
		}
		for i :=range rows{  // 发送到mq
			fmt.Println("================")
			fmt.Println(rows[i])

			body, _ := json.Marshal(rows[i])
			err := rq.Publish(rqOptions.Exchange, rqOptions.RouteKey, false, false, amqp.Publishing{Body: body})
			if err!= nil {
				panic(err)
			}
		}
	}
}

func testMq()  {

	rq := cache.NewMqContext()
	rqOptions := cache.MqOptions{Exchange: "db_sync", ExchangeType: "topic", RouteKey: "statement", Queue: "sm"}
	rq.DeclareExchangeQueue(rqOptions)

	rq.Qos(2, 0, false)
	msgs, err := rq.Consume(rqOptions.Queue, "", true, false, false, false, nil)
	errorlog.CheckErr(err)
	for msg := range msgs {
		rowMap := make(map[string]interface{})
		err = json.Unmarshal(msg.Body, &rowMap)
		errorlog.CheckErr(err)
		fmt.Println("====================\n", rowMap)
	}

}

func testRedis()  {
	ctx := context.Background()
	rd := cache.GetDefaultRedis()

	key := "goRedis"
	rd.Set(ctx, key, "example", 10*time.Second)
	val, err := rd.Get(ctx, key).Result()
	if err!=nil{
		panic(err)
	}
	fmt.Println("-----> Redis-Get: ", key, val)
}



