## 通过阿里的开源组件 canal 和 mq 实现 MySql 数据同步到 ElasticSearch

### 整体架构和流程

使用阿里开源的 `Canal` 数据库同步工具，把 `Mysql` 数据的增删改同步到 `RabbitMQ` 或 `Kafka`，然后从 MQ中拿消息处理再同步到 `ElasticSearch` 中。

![image.png](https://p3-juejin.byteimg.com/tos-cn-i-k3u1fbpfcp/6637b1fe94db4196ad160086c57c54fa~tplv-k3u1fbpfcp-watermark.image)

Mysql --> Canal --> MQ（RabbitMq/Kafka） --> ElasticSearch


### canal 介绍和工作原理

**canal** 主要用途是基于 MySQL 数据库增量日志解析，提供增量数据订阅和消费，简单说就是可以对MySQL的增量数据进行实时同步。

**canal** 会模拟 MySQL 主库和从库的交互协议，从而伪装成 MySQL 的从库，然后向 MySQL 主库发送 dump 协议，MySQL主库收到 dump 请求会向 canal 推送 binlog，canal 通过解析 binlog 将数据同步到其他存储中去。

其实真正需要写代码实现的是：

- canal 同步数据到 MQ（SyncCanal2Mq）
- 消费 MQ 数据存储到 ElasticSearch（SyncMq2Elastic）


### 同步 Mysql 表数据到 ElasticSearch 相关说明

​使用ali的Canal数据库同步工具，把Mysql数据的增删改同步到RabbitMQ，然后从MQ中拿取消息同步到Elastic中：

1. 依赖安装 [Canal](https://github.com/alibaba/canal) 、RabbitMq、MySql、ElasticSearch 7.5

2. 编辑和修改 config/config.json 配置文件

3. 主要入口文件 main/search.go

      - go run main/search.go -m module [paramters]

	- 目录module中的文件函数是主要执行体

	- 包括初始化索引，删除索引，重构索引，获取索引mapping，同步canal数据到mq，同步mq数据到es，web应用

### 举例说明

```
-m string
      (module)需要调用的执行模块: init/delete/rebuild/mapping/db2mq/es4mq/web
-t string
      (table name)表名，init/delete/rebuild/mapping模块必选参数
-d string
      (database name)数据库名，init/delete/rebuild/mapping模块必选参数
-n int
      使用n个协程同步mq数据到es，默认10，es4mq模块可选参数 (default 10)
-p string
      端口号，默认8080，web模块可选参数 (default "8080")
-r string
      (regex)canal获取表的同步数据，canal2mq模块必选参数 (default ".*\\..*")
```

```
如 初始化|删除|重构|mapping 数据库statement的表track索引
go run search.go -m init|delete|rebuild|mapping -t track -d statement
如 同步数据statement中所有表更新数据到mq
go run search.go -m db2mq -r "statement\\..*"
如 10个协程同步mq数据到es
go run search.go -m es4mq -n 10
如 启动search web应用,端口8080
go run search.go -m web -p 8080
```
