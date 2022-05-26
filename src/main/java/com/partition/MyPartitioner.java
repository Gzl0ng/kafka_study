package com.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author 郭正龙
 * @date 2021-10-27
 */
public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //当前主题的分区数(总的)
        Integer integer = cluster.partitionCountForTopic(topic);

        //根据当前传进来的参数，可以根据key，根据value或者主题来进行选择分区号
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
