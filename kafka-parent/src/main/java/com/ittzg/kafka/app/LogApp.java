package com.ittzg.kafka.app;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/31 15:52
 */
public class LogApp {
    static Properties props = new Properties();
    static {
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "hadoop-ip-103:9092");
        // 制定consumer group
        props.put("group.id", "ittzg");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }


    public static void first(){
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList("logFirst"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records){
                String msg = record.value();
                System.out.printf("first:\t"+"offset = %d, key = %s, value = %s%n", record.offset(), record.key(), msg);
                if(msg!=null && msg.contains(">")){
                    String[] split = msg.split(">");
                    msg = split[split.length-1];
                }
                KafkaUtils.sendMsg("logSecond",msg);
            }
        }
    }

    public static void second(){
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList("logSecond"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records){
                String msg = record.value();
                System.out.printf("second:\t"+"offset = %d, key = %s, value = %s%n", record.offset(), record.key(), msg);
            }
        }
    }

    public static void main(String[] args) {
        new Thread(()->{
            LogApp.first();
        }).start();
        new Thread(()->{
            LogApp.second();
        }).start();

    }

}
