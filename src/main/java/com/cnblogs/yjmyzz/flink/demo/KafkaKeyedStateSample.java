package com.cnblogs.yjmyzz.flink.demo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

/**
 * @author 菩提树下的杨过(http : / / yjmyzz.cnblogs.com /)
 */
public class KafkaKeyedStateSample {


    private final static Gson gson = new Gson();
    private final static String SOURCE_TOPIC = "test5";
    private final static String SINK_TOPIC = "test6";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {

        // 1 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义数据
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "test-read-group-4");
        props.put("deserializer.encoding", "GB2312");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> text = env.addSource(new FlinkKafkaConsumer011<>(
                SOURCE_TOPIC,
                new SimpleStringSchema(),
                props));

        // 3. 处理逻辑
        DataStream<Tuple2<String, Long>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                //解析message中的json
                Map<String, String> map = gson.fromJson(value, new TypeToken<Map<String, String>>() {
                }.getType());

                String employee = map.getOrDefault("employee", "");
                String status = map.getOrDefault("status", "");
                Long eventTimestamp = Long.parseLong(map.getOrDefault("event_timestamp", "0"));
                out.collect(new Tuple2<>(employee + ":" + status, eventTimestamp));
            }
        })

                .keyBy(value -> value.f0).flatMap(new RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private ValueState<Long> lastStatusTimestamp = null;
                    private ValueState<Long> lastStatusDuration = null;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor lastStatusTimestampDescriptor = new ValueStateDescriptor("lastStatusTimestampState", Long.class);
                        lastStatusTimestamp = getRuntimeContext().getState(lastStatusTimestampDescriptor);

                        ValueStateDescriptor lastStatusDurationDescriptor = new ValueStateDescriptor("lastStatusDurationState", Long.class);
                        lastStatusDuration = getRuntimeContext().getState(lastStatusDurationDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
                        if (lastStatusTimestamp == null || lastStatusTimestamp.value() == null) {
                            out.collect(new Tuple2<>(value.f0, 0L));
                            lastStatusDuration.update(0L);
                        } else {
                            long duration = value.f1 - lastStatusTimestamp.value();
                            lastStatusDuration.update(lastStatusDuration.value() + duration);
                            out.collect(new Tuple2<>(value.f0, lastStatusDuration.value()));
                        }
                        lastStatusTimestamp.update(value.f1);
                    }
                }).keyBy(value -> value.f0).sum(1);


        // 4. 打印结果
        counts.addSink(new FlinkKafkaProducer010<>("localhost:9092", SINK_TOPIC,
                (SerializationSchema<Tuple2<String, Long>>) element -> ("(" + element.f0 + "," + element.f1 + ")").getBytes()));
        counts.print();

        // execute program
        env.execute("Kafka Streaming WordCount");

    }

}