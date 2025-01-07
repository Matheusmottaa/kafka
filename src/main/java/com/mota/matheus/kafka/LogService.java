package com.mota.matheus.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws InterruptedException {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());
        consumer.subscribe(Pattern.compile("ECOMERCE.*"));

        while(true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                System.out.println("Foram encontrados " + records.count() + " registros.");
                for (var record : records) {
                    System.out.println("---------------------------------------------------");
                    System.out.println("LOG: " + record.topic());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                }
            }
        }
    }

    public static Properties properties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return props;
    }

}
