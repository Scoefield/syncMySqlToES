package controllers

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic/v7"
	"github/guanhg/syncDB-search/cache"
	"github/guanhg/syncDB-search/errorlog"
	schema "github/guanhg/syncDB-search/schema"
	"log"
	"net/http"
	"sort"
	"strconv"
)

func TopTrackHandle(c *gin.Context){
	defer func() {
		if err := recover(); err!=nil{
			c.JSON(http.StatusInternalServerError, gin.H{"message": "server error"})
			log.Println(err)
		}
	}()
	startDate := c.Query("startDate")  // 格式 2018-10-01 00:00:00
	endDate := c.Query("endDate")
	maxSize, _ := strconv.Atoi(c.DefaultQuery("max", "500"))
	pageIdx, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("size", "100"))

	var to int
	if pageIdx < 1 || (pageIdx-1)*pageSize > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"message": "parameters error"})
		return
	}else if pageIdx*pageSize > maxSize {
		to = maxSize
	}else{
		to = pageIdx*pageSize
	}
	from := (pageIdx-1)*pageSize

	stmt := "select id from sm where created_datetime > ? and created_datetime < ?"
	ctx := cache.GetDefaultContext()
	rows, err := ctx.Query(stmt, startDate, endDate)
	errorlog.CheckErr(err)

	var ids []int64
	for _, r := range rows {
		ids = append(ids, r["id"].(int64))
	}

	// 聚合
	TopSize := 100
	TopMap := make(map[int]float32)
	for _, id := range ids {
		q := elastic.NewBoolQuery()
		q.Must(elastic.NewTermQuery("sm_id", id))
		q.Must(elastic.NewTermQuery("medium_type", 0))
		q.Must(elastic.NewRangeQuery("medium_id").Gt(0))

		sumAgg := elastic.NewSumAggregation().Field("weight")
		disAgg := elastic.NewTermsAggregation().Field("medium_id").SubAggregation("weight", sumAgg).Size(TopSize).OrderByAggregation("weight", false)

		// size=0意味着不索引Doc，只做聚合计算
		result, err := schema.NewSearch(q, "sm_record_*").Size(0).Aggregation("TopTrack", disAgg).Do(context.Background())
		errorlog.CheckErr(err)
		aggResult, _ := result.Aggregations["TopTrack"].MarshalJSON()

		TopTrack := make(map[string]interface{})
		err = json.Unmarshal(aggResult, &TopTrack)
		errorlog.CheckErr(err)

		total := getRmbExchangeCurrency(ctx, id)
		for _, b := range TopTrack["buckets"].([]interface{}){
			item := b.(map[string]interface{})
			mediumId := int(item["key"].(float64))
			weight := item["weight"].(map[string]interface{})["value"].(float64)
			TopMap[mediumId] += float32(weight)*total
		}
	}

	TopKeys := rankMapIntFloat(TopMap, true)[from:to]
	var retList []map[string]interface{}
	for _, k := range TopKeys{
		retList = append(retList, map[string]interface{}{"mediumId": k, "money": TopMap[k]})
	}

	c.JSON(http.StatusOK, retList)
	return
}

// 获取报表RMB汇率收益
func getRmbExchangeCurrency(ctx *cache.Context, smId int64) float32  {
	stmt := "select currency_type, total_currency from sm where id = ?"
	rows, err := ctx.Query(stmt, smId)
	errorlog.CheckErr(err)
	if rows==nil {
		panic("Can't find statement record")
	}

	total := rows[0]["total_currency"]
	target := rows[0]["currency_type"]

	if target == "RMB" || target == "" {
		return total.(float32)
	} else {
		stmt = "select cash_buying_rate as rate from sm_exchange_rate where target = ? order by id desc limit 1"
		rows, err = ctx.Query(stmt, target)
		errorlog.CheckErr(err)
		if rows==nil{
			panic("Not exist exchange-ratio: " + target.(string) + " -> RMB")
		}
		rate := rows[0]["rate"]
		return float32(rate.(float64))*total.(float32)
	}
}

// 对map[int]float排序, 返回排序后的key
func rankMapIntFloat(m map[int]float32, reverse bool) []int {
	type kv struct {
		Key   int
		Value float32
	}
	var ss []kv
	for k, v := range m {
		ss = append(ss, kv{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value < ss[j].Value
	})

	if reverse { // 交换数据
		for i, j:=0, len(m)-1; i<j; i, j=i+1, j-1{
			ss[i], ss[j] = ss[j], ss[i]
		}
	}

	ranked := make([]int, len(m))
	for i, item := range ss {
		ranked[i] = item.Key
	}

	return ranked
}











