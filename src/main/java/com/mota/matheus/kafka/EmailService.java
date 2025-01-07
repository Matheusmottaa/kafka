package com.mota.matheus.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    public static void main(String[] args) throws InterruptedException {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());
        consumer.subscribe(Collections.singletonList("ECOMERCE_SEND_EMAIL"));
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if(records.isEmpty())
                continue;

            System.out.println("Encontrei " + records.count() + " registros.");
            for(var record : records) {
                System.out.println("---------------------------------------------------");
                System.out.println("Sending email");
                System.out.println(record.topic());
                System.out.println(record.value());
                System.out.println(record.partition());
                System.out.println(record.offset());

                Thread.sleep(2000);
                System.out.println("Email sent");
            }
        }

    }


    public static Properties properties() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());

        return props;
    }
}
