package com.ittzg.kafka.app;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @email: tazhigang095@163.com
 * @author: ittzg
 * @date: 2019/7/31 15:56
 */
public class KafkaUtils {
    public static void sendMsg(String topic,String msg){
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop-ip-102:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
            }
        });
        producer.close();
    }


}
