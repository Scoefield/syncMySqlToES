package module

import (
	"context"
	"encoding/json"
	"fmt"
	"github/guanhg/syncDB-search/cache"
	"github/guanhg/syncDB-search/errorlog"
	"github/guanhg/syncDB-search/schema"
	"log"
	"runtime"
)

// 初始化索引
func InitDbIndex(tableName, dbName string)  {
	checkParam(tableName, dbName)

	dbContext := cache.GetContext(dbName)
	if dbContext == nil {
		panic("dbContext is nil")
	}
	
	table := schema.SchemaIndex{Name: tableName, Context: dbContext}
	err := table.CreateIndexIfNotExist()
	errorlog.CheckErr(err)
	err = table.IndexAll(10)
	errorlog.CheckErr(err)
}

// 删除索引
func DeleteDbIndex(tableName, dbName string)  {
	checkParam(tableName, dbName)

	table := schema.SchemaIndex{Name: tableName, Context: cache.GetContext(dbName)}
	err := table.DeleteIndexIfExist()
	errorlog.CheckErr(err)
}

// 重构索引
func RebuildDbIndex(tableName, dbName string)  {
	checkParam(tableName, dbName)

	InitDbIndex(tableName, dbName)
}

// 获取索引mapping
func MappingIndex(tableName, dbName string) map[string]interface{} {
	checkParam(tableName, dbName)

	mm, err := schema.ElasticClient.GetMapping().Index(tableName).Pretty(true).Do(context.Background())
	errorlog.CheckErr(err)
	mj, _ := json.Marshal(mm)
	fmt.Println(string(mj))

	return mm
}

// 检测参数
func checkParam(tableName, dbName string)  {
	if tableName == ""{
		errorlog.CheckErr(errorlog.ParameterError, "tableName")
	}

	if dbName == ""{
		errorlog.CheckErr(errorlog.ParameterError, "dbName")
	}
}

func TestSearch()  {
	defer func() {
		if e:=recover(); e!=nil{
			buf := make([]byte, 1<<16)
			runtime.Stack(buf, true)
			log.Printf("TestSearchOne panic, errMsg: %v, stackBuf: %v\n", e, string(buf))
		}
	}()
	table := schema.SchemaIndex{Name: "", Context: cache.GetContext("default")}
	//table.GetOneById()
	table.SearchESData()
}