package com.shf.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.UUID;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/11/10 15:11
 */
public final class KafkaConsumerFactory {

    /**
     * 构建KafkaConsumer实例
     *
     * @param bootstrapServers  kafka 服务列表
     * @param groupId           消费组
     * @param keyDeserializer   key序列化规则
     * @param valueDeserializer value序列化规则
     * @param <K>
     * @param <V>
     * @return kafkaConsumer
     */
    public static <K, V> KafkaConsumer<K, V> createKafkaConsumer(String bootstrapServers, String groupId,
                                                                 Class<K> keyDeserializer, Class<V> valueDeserializer) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        // 采用手动commit避免消息丢失
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 默认从上一次消费结束的位置继续消费
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(config);
    }
}
