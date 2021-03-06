package com.cnblogs.yjmyzz.flink.demo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * @author 菩提树下的杨过(http : / / yjmyzz.cnblogs.com /)
 */
public class KafkaStreamTumblingCustomWindowCount {

    private final static Gson gson = new Gson();
    private final static String SOURCE_TOPIC = "test3";
    private final static String SINK_TOPIC = "test4";
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {

        // 1 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        DataStreamSource<String> text = env.addSource(new FlinkKafkaConsumer011<>(
                SOURCE_TOPIC,
                new SimpleStringSchema(),
                props));

        // 3. 处理逻辑
        DataStream<Tuple3<String, Integer, String>> counts = text.assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {

            @Override
            public TimestampAssigner<String> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new RecordTimestampAssigner<>();
            }

            @Override
            public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<String>() {
                    private long maxTimestamp;
                    private long delay = 100;

                    @Override
                    public void onEvent(String s, long l, WatermarkOutput watermarkOutput) {
                        if (StringUtils.isNullOrWhitespaceOnly(s)) {
                            return;
                        }

                        Map<String, String> map = gson.fromJson(s, new TypeToken<Map<String, String>>() {
                        }.getType());
                        String timestamp = map.getOrDefault("event_timestamp", l + "");
                        maxTimestamp = Math.max(maxTimestamp, Long.parseLong(timestamp));
                    }


                    @Override
                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                        watermarkOutput.emitWatermark(new Watermark(maxTimestamp - delay));
                    }
                };
            }
        }).flatMap(new FlatMapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, String>> out) throws Exception {

                if (StringUtils.isNullOrWhitespaceOnly(value)) {
                    return;
                }

                //解析message中的json
                Map<String, String> map = gson.fromJson(value, new TypeToken<Map<String, String>>() {
                }.getType());

                String word = map.getOrDefault("word", "");
                String eventTimestamp = map.getOrDefault("event_timestamp", "0");
                //获取每个统计窗口的时间（用于显示）
                String windowTime = sdf.format(new Date(TumblingEventTimeDayWindows.getDateBegin(Long.parseLong(eventTimestamp))));
                if (word != null && word.trim().length() > 0) {
                    //收集(类似:map-reduce思路)
                    out.collect(new Tuple3<>(word.trim(), 1, windowTime));
                }

            }
        })
                //按Tuple3里的第0项，即：word分组
                .keyBy(value -> value.f0)
                //按每1分整点开固定窗口计算
                .window(TumblingEventTimeDayWindows.of())
                //然后对Tuple3里的第1项求合
                .sum(1);

        // 4. 打印结果
        counts.addSink(new FlinkKafkaProducer010<>("localhost:9092", SINK_TOPIC,
                (SerializationSchema<Tuple3<String, Integer, String>>) element -> (element.f2 + " (" + element.f0 + "," + element.f1 + ")").getBytes()));
        counts.print();

        // execute program
        env.execute("Kafka Streaming WordCount");

    }
}