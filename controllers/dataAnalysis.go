package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic/v7"
	"github/guanhg/syncDB-search/cache"
	"github/guanhg/syncDB-search/errorlog"
	schema "github/guanhg/syncDB-search/schema"
	"log"
	"net/http"
)

const MaxSize = 24

func OverviewHandle(c *gin.Context) {
	defer func() {
		if err := recover(); err!=nil{
			c.JSON(http.StatusInternalServerError, gin.H{"message": "server error"})
			log.Println(err)
		}
	}()
	userId := c.Param("id")
	stmt := "select * from sm_user_profit where user_id=?"
	ctx := cache.GetDefaultContext()
	rows, err := ctx.Query(stmt, userId)
	errorlog.CheckErr(err)

	playTimes := elastic.NewSumAggregation().Field("play_times")
	netEarn := elastic.NewSumAggregation().Field("net_earn")
	playTimesAgg := elastic.NewTermsAggregation().Field("period").SubAggregation("play_times_sum", playTimes).Size(MaxSize)
	netEarnAgg := elastic.NewTermsAggregation().Field("period").SubAggregation("net_earn_sum", netEarn).Size(MaxSize)

	var retPeriod []string
	var retTotal float32
	var retList []map[string]map[string]interface{}
	for _, row := range rows {
		retPeriod = append(retPeriod, fmt.Sprintf("%d%0.2d", row["year"], row["month"]))
		retTotal += row["recording"].(float32)+row["publishing"].(float32)

		q := elastic.NewTermQuery("up_id", row["id"])
		result, err := schema.NewSearch(q, "sm_record_*").Size(0).Aggregation("NetEarn", netEarnAgg).Aggregation("PlayTimes", playTimesAgg).Do(context.Background())
		errorlog.CheckErr(err)

		NetEarnResult, _ := result.Aggregations["NetEarn"].MarshalJSON()
		PlayTimesResult, _ := result.Aggregations["PlayTimes"].MarshalJSON()

		upTerm := make(map[string]map[string]interface{})

		NetEarn := make(map[string]interface{})
		err = json.Unmarshal(NetEarnResult, &NetEarn)
		errorlog.CheckErr(err)
		for _, b := range NetEarn["buckets"].([]interface{}){
			item := b.(map[string]interface{})
			period := item["key"].(string)
			nes := item["net_earn_sum"].(map[string]interface{})["value"].(float64)
			upTerm[period] = map[string]interface{}{"net_earn_sum": nes}
		}

		PlayTimes := make(map[string]interface{})
		err = json.Unmarshal(PlayTimesResult, &PlayTimes)
		errorlog.CheckErr(err)
		for _, b := range PlayTimes["buckets"].([]interface{}){
			item := b.(map[string]interface{})
			period := item["key"].(string)
			pts := item["play_times_sum"].(map[string]interface{})["value"].(float64)

			if upTerm[period]!=nil {
				upTerm[period]["play_times_sum"] = pts
			}else{
				upTerm[period] = map[string]interface{}{"play_times_sum": pts}
			}
		}

		retList = append(retList, upTerm)
	}

	retMap := make(map[string]interface{})
	retMap["PeriodList"] = retPeriod
	retMap["Total"] = retTotal
	retMap["Items"] = retList

	c.JSON(http.StatusOK, retMap)
	return
}
