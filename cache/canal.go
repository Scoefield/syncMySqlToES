package cache

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/withlin/canal-go/client"
	protocol "github.com/withlin/canal-go/protocol"
	"github/guanhg/syncDB-search/config"
	"log"
)

/*
* 用于数据库的增量同步
*/

type Canal struct {
	Config struct{
		Uri string
		Port int
		UserName string
		Password string
		Dest string
	}

	CanalConn *client.SimpleCanalConnector
}

func GetDefaultCanal() *Canal {
	c := new(Canal)

	cfg := config.JsonConfig
	c.Config.Uri = cfg.Canal.Uri
	c.Config.Port = cfg.Canal.Port
	c.Config.UserName = cfg.Canal.Name
	c.Config.Password = cfg.Canal.Password
	c.Config.Dest = cfg.Canal.Dest

	c.Connecting()

	return c
}

func (c *Canal) Connecting()  {
	connector := client.NewSimpleCanalConnector(
		c.Config.Uri,
		c.Config.Port,
		c.Config.UserName,
		c.Config.Password,
		c.Config.Dest,
		60000,
		60*60*1000)
	err :=connector.Connect()
	if err != nil {
		panic(err)
	}
	log.Println("Connecting To Canal: ", c.Config)
	c.CanalConn = connector
}

func (c *Canal) Get(regex string, size int32) []map[string]interface{} {
	err := c.CanalConn.Subscribe(regex)
	if err != nil {
		panic(err)
	}

	message, err := c.CanalConn.Get(size, nil, nil)
	if err != nil {
		panic(err)
	}

	batchId := message.Id
	if batchId == -1 || len(message.Entries) <= 0 {
		return nil
	}

	var data []map[string]interface{}
	for _, entry := range message.Entries {
		if entry.GetEntryType() == protocol.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == protocol.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(protocol.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err != nil {
			panic(err)
		}
		if rowChange != nil {
			mm := make(map[string]interface{})

			eventType := rowChange.GetEventType()
			header := entry.GetHeader()
			mm["schema"] = header.GetSchemaName()
			mm["table"] = header.GetTableName()

			if eventType == protocol.EventType_ALTER || eventType == protocol.EventType_TRUNCATE ||
				eventType == protocol.EventType_RENAME { // 修改表结构或清除表数据
				mm["event"] = -1
				data = append(data, mm)
			} else if eventType == protocol.EventType_ERASE { // 表删除和新建
				mm["event"] = -2
				data = append(data, mm)
			} else if eventType == protocol.EventType_DELETE || eventType == protocol.EventType_UPDATE ||
				eventType == protocol.EventType_INSERT {  // 表数据更新
				mm["event"] = header.GetEventType()

				var row map[string]interface{}
				for _, rowData := range rowChange.GetRowDatas() {
					if eventType == protocol.EventType_DELETE {
						row = ConvertColumn(rowData.GetBeforeColumns())
					} else if eventType == protocol.EventType_INSERT {
						row = ConvertColumn(rowData.GetAfterColumns())
					} else if eventType == protocol.EventType_UPDATE {
						row = ConvertColumn(rowData.GetAfterColumns())
					}
				}
				mm["data"] = row
				data = append(data, mm)
			}

			fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))
		}
	}
	return data
}

func ConvertColumn(columns []*protocol.Column) map[string]interface{} {
	row := make(map[string]interface{})
	for _, col := range columns {
		row[col.GetName()] = col.GetValue()
	}
	return  row
}

