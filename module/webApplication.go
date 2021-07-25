package module

import (
	"github.com/gin-gonic/gin"
	"github/guanhg/syncDB-search/controllers"
)

func Application(port string) {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.GET("/v2/statement/data-analysis/overview/:id", controllers.OverviewHandle)
	r.GET("/v2/statement/data-analysis/top/track", controllers.TopTrackHandle)
	r.GET("/track/tag", controllers.SearchTagTrack)
	r.Run("0.0.0.0:"+port) // listen and serve on 0.0.0.0:8080
}
