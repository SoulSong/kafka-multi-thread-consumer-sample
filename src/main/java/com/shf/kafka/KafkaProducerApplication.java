package com.shf.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * @author songhaifeng
 */
@SpringBootApplication
public class KafkaProducerApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<String, String> template;

    private final static String SAMPLE_TOPIC_NAME = "sample";

    @Override
    public void run(String... args) throws Exception {
        int recordSize = 300;
        CountDownLatch latch = new CountDownLatch(recordSize);
        IntStream.rangeClosed(1, recordSize).forEach(i -> {
            template.send(SAMPLE_TOPIC_NAME, UUID.randomUUID().toString(), "foo" + i);
            latch.countDown();
        });
        latch.await();
    }

    @Bean
    public NewTopic sample() {
        return TopicBuilder.name(SAMPLE_TOPIC_NAME)
                .partitions(5)
                .replicas(3)
                .compact()
                .build();
    }
}
