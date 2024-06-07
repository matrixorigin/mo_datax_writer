# DataX将MySQL数据导出到MatrixOne

## 环境说明

MatrixOne版本：1.2.0

MySQL版本：8.0.33



## 环境配置

- 完成[单机部署 MatrixOne](https://docs.matrixorigin.cn/1.2.0/MatrixOne/Get-Started/install-standalone-matrixone/)，通过 MySQL 客户端创建数据库。

- 完成MySQL部署。

- 下载[DataX](https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202309/datax.tar.gz)工具。



## 初始化MatrixOne数据表

### 1.创建数据库

```sql
create database test;
```

### 2.创建表

```sql
use test;

CREATE TABLE `user` (
`name` VARCHAR(255) DEFAULT null,
`age` INT DEFAULT null,
`city` VARCHAR(255) DEFAULT null
)
```



## 初始化MySQL数据表

### 1.创建数据库

```SQL
create database test;
```

### 2.创建表

```sql
use test;

CREATE TABLE `user` (
  `name` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `age` int DEFAULT NULL,
  `city` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
```

### 3.导入数据

```sql
insert into user values('zhangsan',26,'Shanghai'),('lisi',24,'Chengdu'),('wangwu',28,'Xian'),('zhaoliu',22,'Beijing'),('tianqi',26,'Shenzhen');
```



## 使用DataX导入数据

### 1.下载并解压DataX

解压后目录如下：

```shell
[root@node03 datax]# ll
total 4
drwxr-xr-x. 2 root root   59 Oct 12  2023 bin
drwxr-xr-x. 2 root root   68 Oct 12  2023 conf
drwxr-xr-x. 2 root root   61 Oct 12  2023 job
drwxr-xr-x. 2 root root    6 Oct 12  2023 jsons
drwxr-xr-x. 2 root root 4096 Oct 12  2023 lib
drwxr-xr-x. 3 root root   24 Oct 12  2023 log
drwxr-xr-x. 3 root root   24 Oct 12  2023 log_perf
drwxr-xr-x. 4 root root   34 Oct 12  2023 plugin
drwxr-xr-x. 2 root root   23 Oct 12  2023 script
drwxr-xr-x. 2 root root   24 Oct 12  2023 tmp
[root@node03 datax]#
```

### 2.添加matrixonewriter插件

将matrixonewriter.zip压缩文件放到datax安装目录下：datax/plugin/writer，并使用 **unzip matrixonewriter.zip** 命令进行解压，解压完成后删除 matrixonewriter.zip  文件。

### 3.编写配置文件

编写datax配置文件**mysql2mo.json**，并放在datax安装目录下的job目录

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
					    // MySQL数据库用户名
                        "username": "root",
						// MySQL数据库密码
                        "password": "root",
						// MySQL数据表读取的列名
                        "column": ["name","age","city"],
                        "splitPk": "",
                        "connection": [
                            {
							    // MySQL数据表
                                "table": ["user"],
								// MySQL连接信息
                                "jdbcUrl": [
                                    "jdbc:mysql://127.0.0.1:3306/test"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "matrixonewriter",
                    "parameter": {
					    // 数据库用户名
                        "username": "root",
						// 数据库密码
                        "password": "111",
						// 需要导入的表列名
                        "column": ["name","age","city"],
						// 导入任务开始前需要执行的SQL语句
                        "preSql": [],
						// 导入任务完成之后要执行的SQL语句
                        "postSql": [],
						// 批量写入条数，即读取多少条数据后执行load data inline导入任务
                        "maxBatchRows": 60000,
						// 批量写入大小，即读取多大的数据后执行load data inline导入任务
                        "maxBatchSize": 5242880,
						// 导入任务执行时间间隔，即经过多长时间后执行load data inline导入任务
                        "flushInterval": 300000,
                        "connection": [
                            {
							    // 数据库连接信息
                                "jdbcUrl": "jdbc:mysql://127.0.0.1:6001/test?useUnicode=true",
								// 数据库名
                                "database": "test",
								// 数据库表
                                "table": ["user"]
                            }
                        ]
                    }
                }
            }
        ]
    }
}
```

### 3.执行DataX任务

进入datax安装目录，执行以下命令

```shell
python bin/datax.py job/mysql2mo.json
```

执行完成后，输出结果如下：

```shell
2024-06-05 11:25:41.453 [job-0] INFO  StandAloneJobContainerCommunicator - Total 5 records, 75 bytes | Speed 7B/s, 0 records/s | Error 0 records, 0 bytes |  All Task WaitWriterTime 0.000s |  All Task WaitReaderTime 0.000s | Percentage 100.00%
2024-06-05 11:25:41.454 [job-0] INFO  JobContainer -
任务启动时刻                    : 2024-06-05 11:25:31
任务结束时刻                    : 2024-06-05 11:25:41
任务总计耗时                    :                 10s
任务平均流量                    :                7B/s
记录写入速度                    :              0rec/s
读出记录总数                    :                   5
读写失败总数                    :                   0
```

### 4.查看执行结果

在MatrixOne数据库中查看结果，可以看到数据已经从MySQL同步到MatrixOne中

```shell
mysql> select * from user;
+----------+------+-----------+
| name     | age  | city      |
+----------+------+-----------+
| zhangsan |   26 | Shanghai  |
| lisi     |   24 | Chengdu   |
| wangwu   |   28 | Xian      |
| zhaoliu  |   22 | Beijing   |
| tianqi   |   26 | Shenzhen  |
+----------+------+-----------+
5 rows in set (0.01 sec)
```
