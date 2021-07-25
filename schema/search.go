package schema

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic"
	"github/guanhg/syncDB-search/errorlog"
)

type SchemaSearch struct {
	*elastic.SearchService   // 结构体继承
	Count *elastic.CountService
}

func buildSchemaSearch() *SchemaSearch {
	search := new(SchemaSearch)
	search.SearchService = ElasticClient.Search()
	search.Count = ElasticClient.Count()
	return search
}

/*
* 数据库表的索引检索
* q 查询对象
* indexName 索引名，可以用正则表示多个索引，如"sm_record_2017,sm_record_2018"或"sm_record_*"或"*"等
 */
func NewSearch(q elastic.Query, indexName string) *SchemaSearch{
	s := buildSchemaSearch()
	s.Index(indexName).Query(q)
	s.Count.Index(indexName).Query(q)
	return s
}

// es最后返回结果
func (s *SchemaSearch) Result() map[string]interface{}{
	ctx := context.Background()

	res, err := s.Do(ctx)
	errorlog.CheckErr(err)
	total, err := s.Count.Do(ctx)
	errorlog.CheckErr(err)

	ret := make(map[string]interface{})
	ret["total"] = total

	hits := make([]interface{}, 0)
	if total > 0 {
		for _, hit := range res.Hits.Hits {
			v := make(map[string]interface{})
			err := json.Unmarshal(*hit.Source, &v)
			errorlog.CheckErr(err)
			hits = append(hits, v)
		}
	}
	ret["items"] = hits
	ret["count"] = len(hits)

	return ret
}

