package com.shf.kafka.job;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/11/12 13:59
 */
public class JobDescription {
    /**
     * 一个job建议一个topic，管理管理
     */
    private String topic;
    /**
     * 消费并行度，其值<=分区数
     */
    private int parallelism;
    /**
     * 创建KafkaConsumer实例数，通常1个即可
     */
    private int consumerSize;
    /**
     * 当前消费者id
     */
    private String groupId;
    /**
     * 每个record处理的最大时长，主要作用域task等待complete时长
     */
    private long recordHandleTimeoutOfMillis = 100;
    /**
     * {@link KafkaConsumer#poll(Duration)}，此参数根据实际情况而调整
     */
    private long pollTimeoutOfMillis = 30 * 1000;

    public JobDescription() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getConsumerSize() {
        return consumerSize;
    }

    public void setConsumerSize(int consumerSize) {
        this.consumerSize = consumerSize;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getRecordHandleTimeoutOfMillis() {
        return recordHandleTimeoutOfMillis;
    }

    public void setRecordHandleTimeoutOfMillis(long recordHandleTimeoutOfMillis) {
        this.recordHandleTimeoutOfMillis = recordHandleTimeoutOfMillis;
    }

    public long getPollTimeoutOfMillis() {
        return pollTimeoutOfMillis;
    }

    public void setPollTimeoutOfMillis(long pollTimeoutOfMillis) {
        this.pollTimeoutOfMillis = pollTimeoutOfMillis;
    }
}
