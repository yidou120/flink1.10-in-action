package com.edou.myflink.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @ClassName KafkaPipelineTest
 * @Description 从kafka读取数据 转换后 输出到kafka
 * @Author 中森明菜
 * @Date 2020/11/14 17:51
 * @Version 1.0
 */
public class KafkaPipelineTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // source kafka
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .property("zookeeper.connect", "192.168.101.129:2181")
                        .property("bootstrap.servers", "192.168.101.129:9092")
        ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("KafkaInputTable");

        // 转换 transformation
        Table sensor = tEnv.from("KafkaInputTable");
        Table resultTable = sensor.select("id,temperature")
                .filter("id==='sensor_1'");

        // 聚合操作 不支持append流到文件/kafka
        Table aggOut = sensor.groupBy("id")
                .select("id, id.count as count");
        // sink kafka
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("sinktest")
                        .property("zookeeper.connect", "192.168.101.129:2181")
                        .property("bootstrap.servers", "192.168.101.129:9092")
        ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("KafkaOutputTable");

        resultTable.insertInto("KafkaOutputTable");

        env.execute("start kafka job");
    }
}
