package com.shf.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * description :
 * 实现此接口即可完成对消息的消费处理
 *
 * @author songhaifeng
 * @date 2020/11/10 15:31
 */
public interface RecordProcessor<K, V, R> {

    /**
     * 实际对record处理，需要包含对所有异常的捕获处理
     *
     * @param consumer 消费者实例
     * @param record   待处理record
     * @return result
     */
    R handle(KafkaConsumer<K, V> consumer, ConsumerRecord<K, V> record);

}
