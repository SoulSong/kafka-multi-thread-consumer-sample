package com.shf.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shf.kafka.consumer.StringMultithreadedKafkaConsumer;
import com.shf.kafka.consumer.multi.MultithreadedKafkaConsumer;
import com.shf.kafka.job.JobDescription;
import com.shf.kafka.processor.LogRecordProcessor;
import com.shf.kafka.thread.ThreadPoolFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/11/10 13:45
 */
@EnableAutoConfiguration(exclude = {KafkaAutoConfiguration.class})
@ComponentScan(basePackages = {"com.shf.kafka"}, excludeFilters =
        {@ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = KafkaProducerApplication.class)})
@PropertySource(value = {"classpath:config/application.properties"})
public class KafkaConsumerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerApplication.class);

    /**
     * 读取任务描述文件，针对每个topic创建对应的consumer并实现多线程消费模型。
     *
     * @param args 第一个参数为描述{@link JobDescription}的json文件路径
     * @throws IOException e
     */
    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            LOGGER.info("Need a job description file path.");
            return;
        }
        final Path path = Paths.get(args[0]);
        final ApplicationContext ctx = new AnnotationConfigApplicationContext(KafkaConsumerApplication.class);
        final String bootstrapServers = ctx.getEnvironment().getProperty("spring.kafka.bootstrap-servers");
        final ObjectMapper objectMapper = ctx.getBean(ObjectMapper.class);

        // 任务描述
        String json = Files.lines(path).collect(Collectors.joining());
        List<JobDescription> jobDescriptions = objectMapper.readValue(json, new TypeReference<List<JobDescription>>() {
        });

        List<MultithreadedKafkaConsumer> consumerList = new ArrayList<>();
        jobDescriptions.forEach(jobDescription -> {
            // 根据每个job描述，构建对应的consumer实例数
            IntStream.range(0, jobDescription.getConsumerSize()).forEach(index -> {
                MultithreadedKafkaConsumer<String, String, Void> consumer = new StringMultithreadedKafkaConsumer(bootstrapServers,
                        // 线程池定义
                        ThreadPoolFactory.createThreadPoolTaskExecutor(jobDescription),
                        jobDescription.getTopic(), jobDescription.getGroupId(),
                        // 处理器定义
                        new LogRecordProcessor(), StringDeserializer.class, StringDeserializer.class,
                        jobDescription.getRecordHandleTimeoutOfMillis(), jobDescription.getPollTimeoutOfMillis());
                new Thread(consumer).start();
                consumerList.add(consumer);
            });
        });
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.error("{}", e);
        } finally {
            // add to shutdown hook
            consumerList.forEach(MultithreadedKafkaConsumer::stopConsuming);
        }
    }

}
