package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic/v7"
	"github/guanhg/syncDB-search/schema"
	"log"
	"net/http"
	"strconv"
)

func SearchTagTrack(c *gin.Context)  {
	defer func() {
		if err := recover(); err!=nil{
			c.JSON(http.StatusInternalServerError, gin.H{"message": "server error"})
			log.Println(err)
		}
	}()

	tag := c.Query("id")
	pageIdx, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("size", "10"))

	from := (pageIdx-1)*pageSize

	q := elastic.NewTermQuery("id", tag)
	search := schema.NewSearch(q, "tag")
	//search.Size(2)
	tags := search.Result()

	rq := elastic.NewBoolQuery()
	for _, item := range tags["items"].([]interface{}) {
		doc := item.(map[string]interface{})
		rq.Should(elastic.NewTermQuery("tag_id", doc["id"]))
	}
	relSearch := schema.NewSearch(rq, "track_tag_rel")
	relSearch.From(from).Size(pageSize)
	rels := relSearch.Result()   // track-tag 关系表结果

	tq := elastic.NewBoolQuery()
	for _, item := range rels["items"].([]interface{}) {
		doc := item.(map[string]interface{})
		tq.Should(elastic.NewTermQuery("id", doc["track_id"]))
	}
	trackSearh := schema.NewSearch(tq, "track")
	trackSearh.From(0).Size(pageSize)
	tracks := trackSearh.Result()

	retMap := make(map[string]interface{})
	retMap["Total"] = rels["total"]
	retMap["Items"] = tracks["items"]
	retMap["Count"] = tracks["count"]

	c.JSON(http.StatusOK, retMap)
	return
}
