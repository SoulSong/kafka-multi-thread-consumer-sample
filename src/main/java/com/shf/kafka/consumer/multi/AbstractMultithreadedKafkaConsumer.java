package com.shf.kafka.consumer.multi;


import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * description :
 * 多线程消费者主执行流程定义与实现.根据业务特性，通常仅需继承当前抽象类，并明确对应的泛型类型即可。
 *
 * @author songhaifeng
 * @date 2020/10/29 17:07
 */
public abstract class AbstractMultithreadedKafkaConsumer<K, V, R> implements MultithreadedKafkaConsumer<K, V, R>, ConsumerRebalanceListener {
    private final Logger LOGGER = LoggerFactory.getLogger(AbstractMultithreadedKafkaConsumer.class);

    private final String topic;
    private final KafkaConsumer<K, V> consumer;
    private final ThreadPoolTaskExecutor executor;
    /**
     * record处理器
     */
    private final RecordProcessor<K, V, R> recordProcessor;
    /**
     * 每个record处理的最大时长，主要作用域task等待complete时长
     */
    private final long recordHandleTimeoutOfMillis;
    /**
     * {@link KafkaConsumer#poll(Duration)}
     */
    private final long pollTimeoutOfMillis;
    /**
     * 存储所有活跃状态的task
     */
    private final Map<TopicPartition, ConsumerWorker> activeTasks = new HashMap<>();
    /**
     * 全局offset收集器
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    /**
     * 当前consumer执行状态标志
     */
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    /**
     * 记录上一次offset提交时间，用于周期性提交比对
     */
    private long lastCommitTime = System.currentTimeMillis();

    public AbstractMultithreadedKafkaConsumer(String bootstrapServers, ThreadPoolTaskExecutor executor,
                                              String topic, String groupId,
                                              RecordProcessor<K, V, R> recordProcessor,
                                              Class<K> keyDeserializer, Class<V> valueDeserializer,
                                              long recordHandleTimeoutOfMillis, long pollTimeoutOfMillis) {
        this.executor = executor;
        this.topic = topic;
        this.recordProcessor = recordProcessor;
        this.recordHandleTimeoutOfMillis = recordHandleTimeoutOfMillis;
        this.pollTimeoutOfMillis = pollTimeoutOfMillis;
        consumer = KafkaConsumerFactory.createKafkaConsumer(bootstrapServers, groupId, keyDeserializer, valueDeserializer);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton(topic), this);
            // 轮训拉取record数据
            while (!stopped.get()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.of(pollTimeoutOfMillis, ChronoUnit.MILLIS));
                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException wakeupException) {
            // 如果当前stop标记为false，则说明并非执行#stop方法的正常停止操作
            if (!stopped.get()) {
                throw wakeupException;
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 根据分区拆分record，并分组提交至线程池处理
     *
     * @param records batch records
     */
    @Override
    public void handleFetchedRecords(ConsumerRecords<K, V> records) {
        if (records.count() > 0) {
            // 处理中的分区将会被暂停下一轮的poll操作，从而保证消息的有序性
            List<TopicPartition> partitionsToPause = new ArrayList<>(records.partitions().size());
            records.partitions()
                    .forEach(partition -> {
                        // 获取当前分区的所有records
                        List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
                        // 构造分区task并提交至线程池
                        ConsumerWorker<K, V, R> task = new ConsumerWorker<>(consumer, partitionRecords, recordProcessor);
                        // 将当前分区加入到pause分区列表
                        partitionsToPause.add(partition);
                        executor.submit(task);
                        // 当前分区task加入到活跃列表
                        activeTasks.put(partition, task);
                    });
            // 暂停处理中分区
            consumer.pause(partitionsToPause);
        }
    }

    /**
     * 查看分区task执行状态
     */
    @Override
    public void checkActiveTasks() {
        // 记录处理完成的分区task
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>(activeTasks.size());
        // 遍历所有活跃任务
        activeTasks.forEach((partition, task) -> {
            // 根据finished状态判断，加入已完成列表
            if (task.isFinished()) {
                finishedTasksPartitions.add(partition);
            }
            // 获取对应task的当前offset
            long offset = task.getCurrentOffset();
            // 当offset大于0， 则记录等待提交
            if (offset > 0) {
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
            }
        });
        // 从活跃任务中移除已完成task
        finishedTasksPartitions.forEach(activeTasks::remove);
        // 恢复已完成分区，即在下一轮即可重新poll对应分区的record
        consumer.resume(finishedTasksPartitions);
    }

    /**
     * 提交offset
     */
    @Override
    public void commitOffsets() {
        try {
            // 避免过于频繁的提交，过设置了一个5s的周期性提交规则
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                // 同步提交所有分区offset，提交后需要清空数据集
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to commit offsets!", e);
        }
    }

    /**
     * 该回调方法将在停止拉取数据之后，rebalance 启动时执行
     *
     * @param partitions 重分配后丢失的分区
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // 停止当前消费者被取消的分区task任务
        Map<TopicPartition, ConsumerWorker> stoppedTask = new HashMap<>(partitions.size());
        for (TopicPartition partition : partitions) {
            // 从活跃列表中移除对应分区task，并立即停止对应的task
            ConsumerWorker task = activeTasks.remove(partition);
            if (task != null) {
                task.stop();
                stoppedTask.put(partition, task);
            }
        }

        // 等待每个work当前处理的record完成，并记录对应的offset
        stoppedTask.forEach((partition, task) -> {
            long offset = task.waitForCompletion(recordHandleTimeoutOfMillis, TimeUnit.MILLISECONDS);
            if (offset > 0) {
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
            }
        });

        // 收集由于分区被取消必须提交offset集合，并从全局offsetToCommit集合中移除
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>(partitions.size());
        partitions.forEach(partition -> {
            OffsetAndMetadata offset = offsetsToCommit.remove(partition);
            if (offset != null) {
                revokedPartitionOffsets.put(partition, offset);
            }
        });

        // 提交待回收的分区offset
        try {
            consumer.commitSync(revokedPartitionOffsets);
        } catch (Exception e) {
            LOGGER.warn("Failed to commit offsets for revoked partitions!");
        }
    }

    /**
     * 分区重分配完成后回调执行
     *
     * @param partitions 被分配到的所有分区
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 恢复所有分区
        consumer.resume(partitions);
    }

    /**
     * 停止当前consumer
     */
    @Override
    public void stopConsuming() {
        stopped.set(true);
        // 终止当前的poll
        consumer.wakeup();
    }

}