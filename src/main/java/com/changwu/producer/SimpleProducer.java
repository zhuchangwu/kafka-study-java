package com.changwu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducer {

    // Producer异步发送消息
    public static void producerSend() throws ExecutionException, InterruptedException {
        // Producer的相关配置
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // kafka按照批次发送消息，batch_size指定每批数的大小
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // 长时间发送一个批次，1毫秒
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_"MEMORY_CONFIG, "33554432");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.changwu.producer.SimplePartitioner");
        // 构建Producer，发消息的客户端
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 构建需要发送的消息的封装体，有多个重载，如支持指定Partition
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("Order22", "orderId", "4");
        // 发送消息
        // 确保kafka将需要异步发送的消息发送出去前，本程序不要运行结束，否则消息发送失败
        Future<RecordMetadata> future = producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println("partition "+recordMetadata.partition());
                System.out.println("offSet "+recordMetadata.offset());
            }
        });
        // 使用完send方法后，需关闭producer释放资源
        producer.close();
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        producerSend();
    }


}
