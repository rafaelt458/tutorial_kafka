package com.laboratorio.democonsumerapi;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DemoConsumerApi {
    private static final Logger log = LogManager.getLogger(DemoConsumerApi.class);
    
    public static void main(String[] args) {
        String topic = "mastodon_topic";
        log.info("Iniciando el programa Consumidor de Kafka");
        
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.69:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Mastodon_3");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        
        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(properties);
        consumidor.subscribe(Collections.singletonList(topic));
        log.info("Me he suscrito al topic: {}", topic);
        
        ConsumerRecords<String, String> records;
        while (true) {
            records = consumidor.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                log.info("No se recibieron mensajes");
            } else {
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Mensaje recibido: offset = {}, key = {}, value = {}, partición = {}", 
                            record.offset(), record.key(), record.value(), record.partition());
                }
            }
        }
        
        // log.info("Finalizando el programa Consumidor de Kafka");
    }
}
