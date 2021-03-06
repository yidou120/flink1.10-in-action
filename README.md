### 环境
1. kafka 2.11-2.4.1
2. flink 1.10
### 命令
1. 启动zookeeper(kafka压缩包里自带zookeeper)
```java
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. 启动kafka
```java
bin/kafka-server-start.sh config/server.properties
```
3. 创建一个名为sensor的topic
```java
bin/kafka-topics.sh --create --topic sensor --bootstrap-server localhost:9092
```
4. 给指定topic发送消息
- kafka2.5之后的命令(包含2.5)
```java
bin/kafka-console-producer.sh --topic sensor --bootstrap-server localhost:9092
```
- kafka2.5之前的命令
```java
bin/kafka-console-producer.sh --topic sensor --broker-list localhost:9092
```
5. 指定topic接受/消费消息
```java
bin/kafka-console-consumer.sh --topic sinktest --from-beginning --bootstrap-server localhost:9092 
```
### 注意
1. 安装了kafka之后需要修改conf下的server.properties这两个属性
```java
# 允许外部端口连接                                            
listeners=PLAINTEXT://0.0.0.0:9092  
# 外部代理地址                                                
advertised.listeners=PLAINTEXT://121.201.64.12:9092
```