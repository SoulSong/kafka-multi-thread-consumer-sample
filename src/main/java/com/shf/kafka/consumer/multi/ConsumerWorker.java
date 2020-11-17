package com.shf.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * description :
 * 处理每个分区的records，并实时更新offset，以便汇报给KafkaConsumer实例
 *
 * @author songhaifeng
 * @date 2020/10/29 17:09
 */
public final class ConsumerWorker<K, V, R> implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWorker.class);

    /**
     * 消费者实例
     */
    private KafkaConsumer<K, V> consumer;

    /**
     * 分区消息集合
     */
    private final List<ConsumerRecord<K, V>> records;

    /**
     * record处理器
     */
    private final RecordProcessor<K, V, R> recordProcessor;

    private volatile boolean stopped = false;

    private volatile boolean started = false;

    private volatile boolean finished = false;

    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    private final ReentrantLock startStopLock = new ReentrantLock();

    /**
     * 记录待提交的offset，默认初始为0
     */
    private final AtomicLong currentOffset = new AtomicLong();

    public ConsumerWorker(KafkaConsumer<K, V> consumer, List<ConsumerRecord<K, V>> records, RecordProcessor<K, V, R> recordProcessor) {
        this.consumer = consumer;
        this.records = records;
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void run() {
        // 防止任务被重复提交
        startStopLock.lock();
        try {
            if (stopped) {
                return;
            }
            started = true;
        } finally {
            startStopLock.unlock();
        }

        // 每个record处理完成均需要设置对应的offset，
        // 从而在stop当前task后仅需等待当前record处理完成即可，无需等待所有record完成
        // 缩短等待处理时长
        for (ConsumerRecord<K, V> record : records) {
            // 在执行stop方法后则跳出循环，不进行后续record处理
            if (stopped) {
                break;
            }
            // 处理record
            recordProcessor.handle(consumer, record);
            // 更新待消费的offset，kafka的offset记录下一条待消费record，故为当前offset+1
            currentOffset.set(record.offset() + 1);
        }
        // 标记完成，并设置当前的offset
        finished = true;
        completion.complete(getCurrentOffset());
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    /**
     * 设置停止处理标志
     */
    public void stop() {
        startStopLock.lock();
        try {
            this.stopped = true;
            // 如果当前task并未启动，则直接标记为已完成。提交任务后就会立即被设置为true，故通常不会走入此逻辑
            if (!started) {
                finished = true;
                // 此时offset为0值，为了避免已经提交的offset被重置为0，下游会进行>0判断
                completion.complete(getCurrentOffset());
            }
        } finally {
            startStopLock.unlock();
        }
    }

    /**
     * 等待当前record处理完成，其约定了最大等待时长。
     * 如果超时完成，则返回的offset -1不会被提交，故在分区重分配后对应消息会被重复消费。
     *
     * @return 处理完成后待提交的offset；-1表示等待超时
     */
    public long waitForCompletion(long timeout, TimeUnit unit) {
        try {
            return completion.get(timeout, unit);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return -1;
        }
    }

    /**
     * 获取当前分区对应批次records是否全部完全消费
     *
     * @return true表示消费完成
     */
    public boolean isFinished() {
        return finished;
    }

}