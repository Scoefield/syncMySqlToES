package cache

import (
	"context"
	rds "github.com/go-redis/redis/v8"
	"github/guanhg/syncDB-search/config"
	"log"
)

type RedisContext struct {
	Config struct{
		Url string
		Pwd string
		Db int
	}

	*rds.Client
}

func NewRedisContext(addr string, pwd string, db int) *RedisContext{
	c := rds.NewClient(&rds.Options{Addr: addr, Password: pwd, DB: db})
	_, err := c.Ping(context.Background()).Result()
	if err!=nil{
		log.Println("Error Connecting to Redis: ", addr)
		panic(err)
	}

	rc := new(RedisContext)
	rc.Config.Url = addr
	rc.Config.Pwd = pwd
	rc.Config.Db = db
	rc.Client = c
	return rc
}

func GetDefaultRedis() *RedisContext{
	cfg := config.JsonConfig.Redis
	return NewRedisContext(cfg.Uri, cfg.Password, cfg.Db)
}

