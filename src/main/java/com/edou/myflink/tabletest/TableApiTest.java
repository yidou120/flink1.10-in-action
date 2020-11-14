package com.edou.myflink.tabletest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import java.io.File;


/**
 * @ClassName TableApiTest
 * @Description 从文件系统/kafka读取数据 经过table api查询 控制台打印输出
 * @Author 中森明菜
 * @Date 2020/11/14 13:26
 * @Version 1.0
 */
public class TableApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String property = System.getProperty("user.dir");
        String resources = "src/main/resources/";
        String path = property + File.separator + resources + "file/sensor.txt";
        // source filesystem
       /* tEnv.connect(
                new FileSystem()
                        .path(path)
        ).withFormat(
                new Csv()
        ).withSchema(
                new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
        ).createTemporaryTable("inputTable");*/

        // source kafka
        tEnv.connect(
                new Kafka()
                .version("universal")
                .topic("sensor")
                .property("zookeeper.connect", "192.168.101.129:2181")
                .property("bootstrap.servers", "192.168.101.129:9092")
        ).withFormat(
                new Csv()
        ).withSchema(
                new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
        ).createTemporaryTable("kafkaInputTable");

        TupleTypeInfo<Tuple2<String,Double>> tuple2TupleTypeInfo = new TupleTypeInfo<>(
                Types.STRING,
                Types.DOUBLE
        );
//        Table sensorTable = tEnv.from("inputTable");
//        Table out = sensorTable.select("id,temperature").filter("id === 'sensor_1'");
//        tEnv.toAppendStream(out,tuple2TupleTypeInfo).print("result");

        Table kafkaInputTable = tEnv.from("kafkaInputTable");
        Table kafkaOut = kafkaInputTable.select("id,temperature").filter("id === 'sensor_1'");
        tEnv.toAppendStream(kafkaOut,tuple2TupleTypeInfo).print();

        env.execute("start...");
    }
}
