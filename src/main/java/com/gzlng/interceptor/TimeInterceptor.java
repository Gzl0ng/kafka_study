package com.gzlng.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author 郭正龙
 * @date 2021-10-28
 */
public class TimeInterceptor implements ProducerInterceptor {
    @Override
    public void configure(Map<String, ?> configs) {

    }

    //对传送数据进行修改
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        //1.取出数据
        Object value = record.value();
        //2.创建新的producerRecord对象,并返回
        return new ProducerRecord<>(record.topic(),record.partition(), record.key(),System.currentTimeMillis() + "." + value);
    }

    //传送后的数据进行修改
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }
}
