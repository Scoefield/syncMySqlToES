package main

import (
	"flag"
	"fmt"
	"github/guanhg/syncDB-search/module"
)

var mod = flag.String("m", "web", "(module)需要调用的执行模块: init/delete/rebuild/mapping/db2mq/es4mq/web")
var tb = flag.String("t", "", "(table name)表名，init/delete/rebuild/mapping模块必须要的参数")
var db = flag.String("d", "", "(database name)数据库名，init/delete/rebuild/mapping模块必须要的参数")
var reg = flag.String("r", ".*\\..*", "(regex)canal获取表的同步数据，canal2mq模块必须要的参数")
var n = flag.Int("n", 10, "使用n个协程同步mq数据到es，默认10，es4mq模块可选参数")
var port = flag.String("p", "8080", "端口号，默认8080，web模块可选参数")

func main() {
	flag.Parse()

	switch *mod {
		case "init":
			module.InitDbIndex(*tb, *db)
		case "delete":
			module.DeleteDbIndex(*tb, *db)
		case "rebuild":
			module.RebuildDbIndex(*tb, *db)
		case "mapping":
			module.MappingIndex(*tb, *db)
		case "db2mq":
			module.SyncCanal2Mq(*reg)
		case "es4mq":
			module.SyncElastic4Mq(*n)
		case "web":
			module.Application(*port)
		case "searches":
			module.TestSearch()
		default:
			fmt.Printf("Module '%s' 不存在，可选 'init/delete/rebuild/mapping/db2mq/es4mq/web'\n", *mod)
	}
}
