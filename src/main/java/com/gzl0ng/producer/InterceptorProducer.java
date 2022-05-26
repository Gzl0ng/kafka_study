package com.gzl0ng.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 郭正龙
 * @date 2021-10-28
 */
public class InterceptorProducer {
    public static void main(String[] args) throws InterruptedException {
        //1.创建kafka生产者的配置信息
        Properties properties = new Properties();

        //2.连接zookeeper集群
        properties.put("bootstrap.servers", "node1:9092");

        //3.ack应答级别
        properties.put("acks", "all");

        //4.重试次数
        properties.put("retries", 1);

        //5.批次大小
        properties.put("batch.size", 16384);

        //6.等待时间
        properties.put("linger.ms", 1);

        //7.RecordAccumulator 缓冲区大小32M
        properties.put("buffer.memory", 33554432);

        //8.Key,Value的序列化类
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //添加拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.gzlng.interceptor.TimeInterceptor");//输出数据加了个时间戳
        interceptors.add("com.gzlng.interceptor.CounterInterceptor");//对数据没有更改，但是数据传送完后输出传送情况
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //9.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //10.发送数据(实际生产，加try-cath-finally，finally中close)
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first","atguigu","atguigu--" + i));
        }

        Thread.sleep(5000);

        System.out.println("***************");

        producer.close();
    }
}
