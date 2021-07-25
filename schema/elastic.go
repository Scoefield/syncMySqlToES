package schema

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"github/guanhg/syncDB-search/config"
	"log"
	"os"
	"time"
)

var (
	ElasticClient = NewElasticClient()
)

func NewElasticClient() *elastic.Client {
	var options []elastic.ClientOptionFunc
	host := config.JsonConfig.ElasticSearch.Host
	options = append(options, elastic.SetURL(host))
	options = append(options, elastic.SetSniff(false))
	options = append(options, elastic.SetGzip(!config.JsonConfig.ElasticSearch.Debug))
	options = append(options, elastic.SetHealthcheck(false))
	options = append(options, elastic.SetHealthcheckTimeout(time.Minute*2))
	options = append(options, elastic.SetErrorLog(log.New(os.Stderr, "[ELASTIC-ERROR] ", log.LstdFlags)))
	if config.JsonConfig.ElasticSearch.Debug {
		options = append(options, elastic.SetInfoLog(log.New(os.Stdout, "[ELASTIC-INFO] ", log.LstdFlags)))
		options = append(options, elastic.SetTraceLog(log.New(os.Stdout, "[ELASTIC-TRACE] ", log.LstdFlags)))
	}

	client, _ := elastic.NewClient(options...)
	info, code, err := client.Ping(host).Do(context.Background())
	if err != nil {
		log.Fatal("Error connecting to elastic: ", host, "\n", err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	esversion, _ := client.ElasticsearchVersion(host)
	fmt.Printf("Elasticsearch version %s\n", esversion)

	return client
}

