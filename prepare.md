# 运行的具体步骤

### 在4个终端分别执行以下命令

```shell
# Start the ZooKeeper service
# Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```

```shell
# Start the Kafka broker service
# kafka的默认端口为9092
$ bin/kafka-server-start.sh config/server.properties
```

```shell
# es的默认端口为9200
$ elasticsearch
```

```shell
# kibana的默认端口为5601，它是es的数据可视化和管理工具。学习es的相关语句在kibana上使用
# http://localhost:5601/
$ kibana
```

