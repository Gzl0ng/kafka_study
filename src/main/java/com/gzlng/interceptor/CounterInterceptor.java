package com.gzlng.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author 郭正龙
 * @date 2021-10-28
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {
    int success;
    int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null){
            success++;
        }else{
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success"+success);
        System.out.println("error"+error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
