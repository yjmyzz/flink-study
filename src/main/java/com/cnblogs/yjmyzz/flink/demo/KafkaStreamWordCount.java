package com.cnblogs.yjmyzz.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author 菩提树下的杨过(http://yjmyzz.cnblogs.com/)
 */
public class KafkaStreamWordCount {

    public static void main(String[] args) throws Exception {

        // 1 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义数据
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test-read-group");
        props.put("deserializer.encoding", "GB2312");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> text = env.addSource(new FlinkKafkaConsumer011<>(
                "test1",
                new SimpleStringSchema(),
                props));

        // 3. 处理逻辑
        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //将每行按word拆分
                String[] tokens = value.toLowerCase().split("\\b+");

                //收集(类似:map-reduce思路)
                for (String token : tokens) {
                    if (token.trim().length() > 0) {
                        out.collect(new Tuple2<>(token.trim(), 1));
                    }
                }
            }
        })
                //按Tuple2里的第0项，即：word分组
                .keyBy(value -> value.f0)
                //然后对Tuple2里的第1项求合
                .sum(1);

        // 4. 打印结果
        counts.addSink(new FlinkKafkaProducer010<>("localhost:9092", "test2",
                (SerializationSchema<Tuple2<String, Integer>>) element -> ("(" + element.f0 + "," + element.f1 + ")").getBytes()));
        counts.print();

        // execute program
        env.execute("Kafka Streaming WordCount");

    }
}