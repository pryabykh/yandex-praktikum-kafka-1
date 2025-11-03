package com.pryabykh.yandex_praktikum_kafka_1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class YandexPraktikumKafka1Application {

	public static void main(String[] args) {
		SpringApplication.run(YandexPraktikumKafka1Application.class, args);
	}

}
