package com.pryabykh.yandex_praktikum_kafka_1.producer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static com.pryabykh.yandex_praktikum_kafka_1.constant.KafkaConstants.BOOTSTRAP_SERVERS;
import static com.pryabykh.yandex_praktikum_kafka_1.constant.KafkaConstants.TOPIC_NAME;

@Component
public class ProducerComponent {
    private static final Logger log = LoggerFactory.getLogger(ProducerComponent.class);
    private KafkaProducer producer;

    @PostConstruct
    void initProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }

    @Scheduled(cron = "* * * * * *")
    public void produce() {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC_NAME,
                UUID.randomUUID().toString(),
                createRandomWeather()
        );
        producer.send(record);
    }

    @PreDestroy
    void closeProducer() {
        log.info("Закрываем продьюсера");
        producer.close();
    }

    private String createRandomWeather() {
        Random random = new Random();
        double temperature = -40.0 + (50.0 - (-40.0)) * random.nextDouble();
        return String.valueOf(Math.round(temperature * 10.0) / 10.0);
    }
}
