package com.cnblogs.yjmyzz.flink.demo;

import akka.japi.tuple.Tuple3;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author 菩提树下的杨过(http : / / yjmyzz.cnblogs.com /)
 */
public class KafkaStreamTumblingWindowCount {

    private final static Gson gson = new Gson();
    private final static String SOURCE_TOPIC = "test3";
    private final static String SINK_TOPIC = "test4";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {

        // 1 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定使用eventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 定义数据
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test-read-group-2");
        props.put("deserializer.encoding", "GB2312");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        // 3.处理逻辑
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> counts = env
                .addSource(
                        new FlinkKafkaConsumer011<>(
                                SOURCE_TOPIC,
                                new SimpleStringSchema(),
                                props))
                .map((MapFunction<String, WordCountPojo>) value -> {
                    WordCountPojo pojo = gson.fromJson(value, WordCountPojo.class);
                    return pojo;
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<WordCountPojo>(Time.milliseconds(200)) {
                            @Override
                            public long extractTimestamp(WordCountPojo element) {
                                return element.eventTimestamp;
                            }
                        })
                .flatMap((FlatMapFunction<WordCountPojo, Tuple3<String, Integer, String>>) (value, out) -> {
                    String word = value.word;
                    //获取每个统计窗口的时间（用于显示）
                    String windowTime = sdf.format(new Date(TimeWindow.getWindowStartWithOffset(value.eventTimestamp, 0, 60 * 1000)));
                    if (word != null && word.trim().length() > 0) {
                        //收集(类似:map-reduce思路)
                        out.collect(new Tuple3<>(word.trim(), 1, windowTime));
                    }
                })
                .keyBy(v -> v.t1())
                .timeWindow(Time.minutes(1))
                .sum(1);

        // 4. 打印结果
        counts.addSink(new FlinkKafkaProducer010<>("localhost:9092", SINK_TOPIC,
                (SerializationSchema<Tuple3<String, Integer, String>>) element -> (element.t3() + " (" + element.t1() + "," + element.t2() + ")").getBytes()));
        counts.print();

        // execute program
        env.execute("Kafka Streaming WordCount");

    }
}