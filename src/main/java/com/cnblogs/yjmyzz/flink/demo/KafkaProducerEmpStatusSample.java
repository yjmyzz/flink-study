package com.cnblogs.yjmyzz.flink.demo;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author 菩提树下的杨过
 */
public class KafkaProducerEmpStatusSample {

    private static String topic = "test5";

    private static Gson gson = new Gson();

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        String[] status = new String[]{"online", "offline"};
        String[] employee = new String[]{"jerry", "mike"};
        Random rnd = new Random();
        try {
            while (true) {
                Map<String, String> map = new HashMap<>();
                map.put("status", status[rnd.nextInt(status.length)]);
                map.put("employee", employee[rnd.nextInt(status.length)]);
                long timestamp = System.currentTimeMillis();
                map.put("event_timestamp", timestamp + "");
                map.put("event_datetime", sdf.format(new Date(timestamp)));
                String msg = gson.toJson(map);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                kafkaProducer.send(record);
                System.out.println(msg);
                Thread.sleep(10000);
            }
        } finally {
            kafkaProducer.close();
        }
    }
}
