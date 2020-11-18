package com.edou.myflink.streamtest;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName BasicStreamFilter
 * @Description 过滤出成年人
 * @Author 中森明菜
 * @Date 2020/11/18 16:38
 * @Version 1.0
 */
public class BasicStreamFilter {
    // ctrl + f12 / alt + 7 显示一个类的所有方法和变量
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Person> people = new ArrayList<>();

        people.add(new Person("Fred", 35));
        people.add(new Person("Wilma", 35));
        people.add(new Person("Pebbles", 2));
        // socket接受数据
//        env.socketTextStream("localhost",888);
        // 从文件中接受数据
//        env.readTextFile("");
        SingleOutputStreamOperator<Person> out = env.fromCollection(people).filter(
                new FilterFunction<Person>() {
                    @Override
                    public boolean filter(Person person) throws Exception {
                        return person.age > 18;
                    }
                }
        );
        out.print();

        env.execute("job start");
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person() {
        }

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
