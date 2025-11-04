package com.pryabykh.yandex_praktikum_kafka_1.producer;

import com.pryabykh.yandex_praktikum_kafka_1.dto.WeatherMessageDto;
import com.pryabykh.yandex_praktikum_kafka_1.mapper.WeatherMessageHelper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

import static com.pryabykh.yandex_praktikum_kafka_1.constant.KafkaConstants.TOPIC_NAME;

@Component
public class ProducerComponent {
    private static final Logger log = LoggerFactory.getLogger(ProducerComponent.class);
    @Autowired
    private WeatherMessageHelper weatherMessageHelper;
    private KafkaProducer producer;
    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @PostConstruct
    void initProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }

    @Scheduled(cron = "* * * * * *")
    public void produce() {
        String randomWeather = weatherMessageHelper.serialize(new WeatherMessageDto());
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC_NAME,
                UUID.randomUUID().toString(),
                randomWeather
        );
        producer.send(record);
        log.info("Сообщение {} отправлено", randomWeather);
    }

    @PreDestroy
    void closeProducer() {
        log.info("Закрываем продьюсера");
        producer.close();
    }
}
