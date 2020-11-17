package com.shf.kafka.thread;

import com.shf.kafka.job.JobDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/11/17 16:28
 */
public final class ThreadPoolFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolFactory.class);

    /**
     * 根据任务描述信息构建线程池
     *
     * @param jobDescription {@link JobDescription}
     * @return thread-pool
     */
    public static ThreadPoolTaskExecutor createThreadPoolTaskExecutor(JobDescription jobDescription) {
        LOGGER.info("Create {} consumers in the group named {} to listen {} with {} parallelisms", jobDescription.getConsumerSize(),
                jobDescription.getGroupId(), jobDescription.getTopic(), jobDescription.getParallelism());
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(jobDescription.getParallelism());
        executor.setMaxPoolSize(jobDescription.getParallelism());
        executor.setKeepAliveSeconds(600);
        executor.setQueueCapacity(100);
        executor.setAwaitTerminationSeconds(120);
        executor.setThreadNamePrefix(jobDescription.getGroupId() + "-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }
}
