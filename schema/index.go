package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"github/guanhg/syncDB-search/cache"
	"github/guanhg/syncDB-search/errorlog"
	"log"
	"reflect"
	"strconv"
	"sync"
)

type SchemaIndex struct {
	Name        string         // 数据库表名
	Context     *cache.Context // 数据库连接
}

// 如果不存在就新建索引
func (s *SchemaIndex) CreateIndexIfNotExist() error {
	indexName := s.Name

	isExists, _ := ElasticClient.IndexExists(indexName).Do(context.Background())
	ctx := context.Background()
	if !isExists {
		_, err := ElasticClient.CreateIndex(indexName).Do(ctx)
		if err!=nil{
			return err
		}

		fieldMapping, err := s.BuildFieldMapping()
		if err != nil {
			return err
		}
		_, err = ElasticClient.PutMapping().Index(indexName).BodyString(fieldMapping).Do(context.Background())
		if err!=nil{
			return err
		}
	}
	return nil
}

// 如果存在就删除索引
func (s *SchemaIndex) DeleteIndexIfExist() error {
	indexName := s.Name

	isExists, _ := ElasticClient.IndexExists(indexName).Do(context.Background())
	if isExists {
		rsp, err := ElasticClient.DeleteIndex().Index([]string{indexName}).Do(context.Background())
		if err!=nil{
			return err
		}
		log.Printf("[Deleting Index] '%s'; Result: %t\n", indexName, rsp.Acknowledged)
	}
	return nil
}

/*
* 构建数据库表的索引
* offset 查询数据库表记录的偏移
* limit 查询数据库表的页大小
*/
func (s *SchemaIndex) Index(offset int, limit int) error {
	log.Printf("[Starting Index] %s - Offset: %d, Limit: %d \n", s.Name, offset, limit)

	indexName := s.Name
	if err:=s.CreateIndexIfNotExist(); err!=nil{
		return err
	}

	stmt := fmt.Sprintf("select * from %s limit ? offset ?", s.Name)
	rows, _ := s.Context.Query(stmt, limit, offset)

	bulkRequest := ElasticClient.Bulk()
	for _, row := range rows {
		id := fmt.Sprintf("%v", row["id"])
		doc := elastic.NewBulkIndexRequest().Index(indexName).Id(id).Doc(row)
		bulkRequest = bulkRequest.Add(doc)
	}
	response, err := bulkRequest.Do(context.Background())
	if err!=nil {
		return err
	}
	failed := response.Failed()
	l := len(failed)
	if l > 0 {
		fmt.Printf("Error(%d)\n", l)
	}

	log.Printf("[Ending Index] %s - Offset: %d, Limit: %d \n", s.Name, offset, limit)
	return nil
}

// 根据数据表的ID索引
func (s *SchemaIndex) IndexOne(id int) error {
	indexName := s.Name
	if err:=s.CreateIndexIfNotExist(); err!=nil{
		return err
	}

	stmt := fmt.Sprintf("select * from %s where id=?", s.Name)
	rows, _ := s.Context.Query(stmt, id)
	for _, row := range rows {
		id := fmt.Sprintf("%v", row["id"])
		r, err := ElasticClient.Index().Index(indexName).Id(id).BodyJson(row).Do(context.Background())
		if err!=nil{
			return err
		}
		log.Printf("[Ending Index] Id: %d; Index %s; Result: %s\n", id, indexName, r.Result)
	}

	return nil
}

// 多协程索引整张表(WaitGroup和chan组合实现协程池)
func (s *SchemaIndex) IndexAll(nunRoutine int) error {
	pageSize := 10000
	total, _ := s.GetCount()
	loops := total/pageSize + 1

	_ = s.DeleteIndexIfExist()
	_ = s.CreateIndexIfNotExist()

	log.Printf("==========%d===========\n", total)
	wg := new(sync.WaitGroup)
	ch := make(chan bool, nunRoutine)
	for i:=0; i<loops; i++ {
		oft := i*pageSize

		wg.Add(1)  // 为了主进程等待所有协程运行完毕
		ch <- true  // 当协程数达到上限时阻塞协程的创建
		go func() {
			err := s.Index(oft, pageSize)
			<- ch
			wg.Done()
			if err!=nil{
				log.Fatalf("[ERROR] IndexAll- %s: Offset %d Limit %d", s.Name, oft, pageSize)
			}
		}()
	}
	wg.Wait()
	log.Printf("[Index All] '%s' Done\n", s.Name)
	return nil
}

/*
* 更新或插入索引
*/
func (s *SchemaIndex) Upsert(row map[string]interface{}) error{
	indexName := s.Name
	s.CreateIndexIfNotExist()

	r, err := ElasticClient.Update().Index(indexName).Type("_doc").Id(row["id"].(string)).DocAsUpsert(true).Doc(row).Do(context.Background())
	if err!=nil {
		return err
	}
	log.Printf("[Upserting Index] Id: %s; Index: %s; Result: %s\n", row["id"], indexName, r.Result)
	return nil
}

/*
* 删除记录
*/
func (s *SchemaIndex)Delete(rowId string) error {
	indexName := s.Name
	s.CreateIndexIfNotExist()
	r, err := ElasticClient.Delete().Index(indexName).Type("_doc").Id(rowId).Do(context.Background())
	if err!=nil {
		return err
	}
	log.Printf("[Deleting Index] Id: %s; Index: %s; Result: %s\n", rowId, indexName, r.Result)
	return nil
}

/*
* 用于构建es的表索引配置, 7.0以后版本Mapping配置已被独立出来
* 这个Mapping只构建常用数据类型
*/
func (s *SchemaIndex) BuildFieldMapping() (string, error) {
	stmt := fmt.Sprintf("select * from %s limit ?", s.Name)

	rows, err := s.Context.Query(stmt, 1)
	if err != nil {
		return "", err
	}

	typeMap := map[string]map[string]interface{}{
		"string": {"type": "text", "fields": map[string]map[string]interface{}{"keyword": { "type": "keyword", "ignore_above": 256}}},
		"int": {"type": "integer"},
		"int8": {"type": "integer"},
		"int16": {"type": "integer"},
		"int32": {"type": "long"},
		"int64": {"type": "long"},
		"float32": {"type": "float"},
		"float64": {"type": "double"},
		"Time": {"type": "date"},
	}

	fieldMap := make(map[string]map[string]interface{})
	for k, v := range rows[0] {   // map类型 反射
		vType := reflect.TypeOf(v)
		if vType == nil{
			continue
		}

		//如果没有该类型，则默认string类型
		tmpField := typeMap[vType.Name()]
		if tmpField == nil {
			fieldMap[k] = typeMap["string"]
		} else {
			fieldMap[k] = tmpField
		}
		// if fieldMap[k] = typeMap[vType.Name()]; fieldMap[k]==nil{  //如果没有该类型，则默认string类型
		// 	fieldMap[k] = typeMap["string"]
		// }
	}
	mapping := make(map[string]interface{})
	mapping["properties"] = fieldMap  // ES7.0 对于先构建索引，再构建mapping，只需要'propertyies'
	//mapping["mappings"] = map[string]map[string]map[string]string{"properties": fieldMap}
	bytes, err := json.Marshal(mapping)
	errorlog.CheckErr(err)
	jonson := string(bytes)

	return jonson, nil
}

// 获取表的最大ID
func (s *SchemaIndex) GetMaxId() (int, error){
	stmt := fmt.Sprintf("select max(id) as max_id from %s", s.Name)
	rows, err := s.Context.Query(stmt)
	if err!=nil{
		return 0, err
	}
	maxId := rows[0]["max_id"]
	return strconv.Atoi(maxId.(string))
}

// 获取表的数据总数
func (s *SchemaIndex) GetCount() (int, error){
	stmt := fmt.Sprintf("select count(id) as cnt from %s", s.Name)
	fmt.Println("**** sql: ", stmt)
	rows, err := s.Context.Query(stmt)
	if err!=nil{
		return 0, err
	}
	cnt := rows[0]["cnt"]
	return strconv.Atoi(cnt.(string))
}



