package com.pryabykh.yandex_praktikum_kafka_1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Properties;

import static com.pryabykh.yandex_praktikum_kafka_1.constant.KafkaConstants.TOPIC_NAME;

@Component
public class BatchMessageConsumer implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(BatchMessageConsumer.class);
    @Autowired
    private KafkaConsumerRunner kafkaConsumerRunner;
    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Override
    public void run(String... args) {
        runSingleMessageConsumer();
    }

    private void runSingleMessageConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "BatchMessageConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1100");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "999999999");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        kafkaConsumerRunner.startConsuming(consumer, BatchMessageConsumer.class, true);
    }
}
