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
public class SingleMessageConsumer implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);
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
        // подключаемся к брокеру
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // устанавливаем уникальный идентификатор группы
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SingleMessageConsumer");
        // сериализация строковая
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // коммитит будет автоматический (кафка клиент коммитит по расписанию, указанному в auto.commit.interval.ms)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // время, в течение которого консьюмер может не слать признаки жизни брокеру
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        // минимальное количество байтов, которое ожидаем получить от брокера
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "110");
        // время, в течение которого брокер будет ждать накопления минимального количества байтов (если время вышло, то шлет, что есть)
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
        // говорим, что в records хотим всегда видеть одно сообщение (даже если фактически poll получил больше одного)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        kafkaConsumerRunner.startConsuming(consumer, SingleMessageConsumer.class, false);
    }
}
