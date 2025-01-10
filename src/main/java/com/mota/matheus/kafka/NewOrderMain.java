package com.mota.matheus.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        String value = "71234,567,894";

        for(int i=0; i<100; ++i) {

            String key = UUID.randomUUID().toString();
            String email = "Thanks! We are processing your order!";

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "ECOMERCE_NEW_ORDER",
                    key,
                    value
            );

            ProducerRecord<String, String> emailRecord = new ProducerRecord<>(
                    "ECOMERCE_SEND_EMAIL",
                    key,
                    email
            );

            Callback callback = (data, ex) -> {
                if(ex!=null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.printf("Send with Success %s ::: partition %d / offset %d /%s%n",
                        data.topic(), data.partition(), data.offset(), data.timestamp());
            };

            producer.send(record, callback).get();     // metodo assincrono, chamando o metodo get voce espera o future terminar;
            producer.send(emailRecord, callback).get();

        }
    }

    public static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
