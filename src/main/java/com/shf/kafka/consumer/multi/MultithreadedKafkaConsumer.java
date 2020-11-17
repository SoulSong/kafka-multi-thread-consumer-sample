package com.shf.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * description :
 * 多线程消费者行为定义
 *
 * @author songhaifeng
 * @date 2020/11/10 17:44
 */
public interface MultithreadedKafkaConsumer<K, V, R> extends Runnable {

    /**
     * 处理批量拉取的records
     *
     * @param records batch records
     */
    void handleFetchedRecords(ConsumerRecords<K, V> records);

    /**
     * 提交offset
     */
    void commitOffsets();

    /**
     * 检测活跃的worker
     */
    void checkActiveTasks();

    /**
     * 停止消费
     */
    void stopConsuming();
}
