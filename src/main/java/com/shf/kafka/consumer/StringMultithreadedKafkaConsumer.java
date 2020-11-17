package com.shf.kafka.consumer;

import com.shf.kafka.consumer.multi.AbstractMultithreadedKafkaConsumer;
import com.shf.kafka.consumer.multi.RecordProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/11/10 17:00
 */
public class StringMultithreadedKafkaConsumer extends AbstractMultithreadedKafkaConsumer<String, String, Void> {

    public StringMultithreadedKafkaConsumer(String bootstrapServers, ThreadPoolTaskExecutor executor, String topic, String groupId,
                                            RecordProcessor recordProcessor, Class keyDeserializer, Class valueDeserializer,
                                            long recordHandleTimeoutOfMillis, long pollTimeoutOfMillis) {
        super(bootstrapServers, executor, topic, groupId, recordProcessor, keyDeserializer, valueDeserializer,
                recordHandleTimeoutOfMillis, pollTimeoutOfMillis);
    }

}
