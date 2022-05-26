package com.gzl0ng.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 郭正龙
 * @date 2021-10-27
 */
public class CallBackProducers {

    public static void main(String[] args) {
        //1.创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //2.创建生产者对象
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        ArrayList<Object> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");


        //3.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("aaa",list.get(i % 3).toString(),"atguigu--" + i), (metadata, exception) -> {
                if (exception == null){
                    System.out.println(metadata.partition()+"--" + metadata.offset());
                }else {
                    exception.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
