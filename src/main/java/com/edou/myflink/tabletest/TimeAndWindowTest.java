package com.edou.myflink.tabletest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.io.File;

/**
 * @ClassName TimeAndWindowTest
 * @Description 使用滚动/滑动窗口进行数据的统计
 * @Author 中森明菜
 * @Date 2020/11/15 12:12
 * @Version 1.0
 */
public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String property = System.getProperty("user.dir");
        String resources = "src/main/resources/";
        String path = property + File.separator + resources + "file/sensor.txt";

        DataStreamSource<String> dataStreamSource = env.readTextFile(path);
        dataStreamSource.flatMap(
                new Splitter()
        );

        env.execute("job start");
    }
    static class Splitter implements FlatMapFunction<String, Tuple3<String,Long,Double>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<String, Long, Double>> collector) throws Exception {
            String[] strings = value.split(",");
            collector.collect(new Tuple3<String, Long, Double>(strings[0],Long.parseLong(strings[1]),Double.parseDouble(strings[2])));
        }
    }
}
