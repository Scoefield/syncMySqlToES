package cache

import (
	"database/sql"
	"errors"
	"fmt"
	"github/guanhg/syncDB-search/config"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Context struct {
	Config struct {
		Uri      string
		Username string
		Password string
		Db       string
	}
	DbConn *sql.DB
}

var (
	contextMap = make(map[string] *Context)
)

const shortDtForm = "2006-01-02 15:04:05"
const shortDForm = "2006-01-02"

func GetContext(env string) *Context{
	ctx := contextMap[env]
	if ctx != nil {
		return ctx
	}
	cfg := config.JsonConfig
	for _, ds := range cfg.DataSource {
		if ds.Name == env {
			ctx := new(Context)
			ctx.Config.Uri = ds.Url
			ctx.Config.Username = ds.User
			ctx.Config.Password = ds.Password
			ctx.Config.Db = ds.Db
			ctx.Connecting()
			contextMap[ds.Name] = ctx
			return ctx
		}
	}

	return nil
}

func GetDefaultContext() *Context{
	return GetContext("default")
}

func (ctx *Context) Close() {
	err := ctx.DbConn.Close()
	if err != nil{
		log.Print("error close!")
		panic(err)
	}
}

func (ctx *Context) Connecting() {
	sqlUri := fmt.Sprintf("%s:%s@tcp(%s)/%s", ctx.Config.Username, ctx.Config.Password, ctx.Config.Uri, ctx.Config.Db)
	db, err := sql.Open("mysql", sqlUri)
	if err != nil {
		log.Print("[Connecting] error: [" + sqlUri+"]\n", err)
		panic(err)
	}
	ctx.DbConn = db
}

func (ctx *Context) Execute(stmt string, para ...interface{}) sql.Result{
	result, err := ctx.DbConn.Exec(stmt, para...)
	if err != nil {
		log.Print("[Exec] error sql\n", err)
		return nil
	}
	return result
}

func (ctx *Context) Query(stmt string, para ...interface{}) ([] map[string] interface{}, error){
	if ctx == nil {
		log.Println("ctx is nil. ctx.")
		return nil, errors.New("ctx is nil")
	}
	rows, err := ctx.DbConn.Query(stmt, para...)
	if err != nil {
		log.Print("[Query] error sql\n", err)
		return nil, err
	}

	return rows2map(rows)
}

func rows2map(rows *sql.Rows) ([] map[string] interface{}, error){
	defer func() {
		if err := rows.Close(); err!=nil {
			panic(err)
		}
	}()

	cols, _ := rows.Columns()  // 获取列名
	colType, _ := rows.ColumnTypes()
	cSize := len(cols)
	val := make([]interface{}, cSize)
	ptr := make([]interface{}, cSize)   // Scan函数只传指针列表
	for i := range val{       // 关联数据和指针
		ptr[i] = &val[i]
	}

	var list [] map[string] interface{}
	for rows.Next() {
		if err:=rows.Scan(ptr...); err!=nil{
			return nil, err
		}
		item := make(map[string] interface{})
		for i, c:=range cols {
			var v interface{}
			b, ok := val[i].([]byte)  // 字符串
			if ok {
				v = string(b)
				if colType[i].DatabaseTypeName() == "DATETIME"{  // datetime
					v, _ = time.Parse(shortDtForm, string(b))
				}else if colType[i].DatabaseTypeName() == "DATE"{  // date
					v, _ = time.Parse(shortDForm, string(b))
				}
			} else {
				v = val[i]
			}
			item[c] = v
		}
		list = append(list, item)
	}

	return list, nil
}


