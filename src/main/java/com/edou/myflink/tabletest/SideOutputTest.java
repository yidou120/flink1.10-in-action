package com.edou.myflink.tabletest;

import com.edou.myflink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;

/**
 * @ClassName SideOutputTest
 * @Description 测输出流
 * @Author 中森明菜
 * @Date 2020/11/30 19:07
 * @Version 1.0
 */
public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9000);
        String property = System.getProperty("user.dir");
        String resources = "src/main/resources/";
        String path = property + File.separator + resources + "file/sensor.txt";
        DataStreamSource<String> dataStream = env.readTextFile(path);
        SingleOutputStreamOperator<SensorReading> mapOut = dataStream.map(
                new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] strings = value.split(",");
                        return new SensorReading(strings[0], Long.parseLong(strings[1]), Double.valueOf(strings[2]));
                    }
                }
        );

        SingleOutputStreamOperator<SensorReading> sideOutPut = mapOut.process(new FilterProcess(30.0D));
        sideOutPut.print("high");
        sideOutPut.getSideOutput(new OutputTag<SensorReading>("low"){}).print("low");

        env.execute("start job");
    }
    static class FilterProcess extends ProcessFunction<SensorReading, SensorReading> {
        private Double temperature;
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if(value.getTemperature() > temperature) {
                out.collect(value);
            }else {
                ctx.output(new OutputTag<SensorReading>("low"){

                },value);
            }
        }
        public FilterProcess(Double temperature) {
            this.temperature = temperature;
        }
    }
}
