package com.gzl0ng.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author 郭正龙
 * @date 2021-10-27
 */
public class PartitionProducer {
    public static void main(String[] args) throws InterruptedException {

        //1.创建kafka生产者的配置信息
        Properties properties = new Properties();

        //2.连接zookeeper集群
//        properties.put("bootstrap.servers", "node1:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092");

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

        //添加分区器
        properties.put("partitioner.class","com.partition.MyPartitioner");
        //9.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //10.发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", "atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println(metadata.partition() +"--" + metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }

        Thread.sleep(100);
        //11.关闭资源(close方法需要一定时间,可以保证数据发送，如果不关，执行太快导致发送不了)
        producer.close();
    }
}
