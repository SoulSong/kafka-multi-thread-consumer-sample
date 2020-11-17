package com.shf.kafka.processor;

import com.shf.kafka.consumer.multi.RecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/11/10 15:43
 */
public class LogRecordProcessor implements RecordProcessor<String, String, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogRecordProcessor.class);

    @Override
    public Void handle(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record) {
        LOGGER.info("{}-->{}", consumer.groupMetadata().memberId(), record.toString());
        return null;
    }
}
