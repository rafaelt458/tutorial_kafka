package com.laboratorio.demostreamsapi;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DemoStreamsAPI {
    private static final Logger log = LogManager.getLogger(DemoStreamsAPI.class);
    
    public static void main(String[] args) {
        log.info("Iniciando el programa de Stream de Kafka");
        
        String topicEntrada = "mastodon_topic";
        String topicCastellano = "mastodon_out_castellano";
        String topicFrances = "mastodon_out_frances";
        String topicIngles = "mastodon_out_ingles";
        
        // Configuración del Stream
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "MESSAGE-FILTER");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.69:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        log.info("Configuración del Stream establecida");
        
        // Definición del Stream
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> fuente = streamsBuilder.stream(topicEntrada, Consumed.with(Serdes.String(), Serdes.String()));
        
        fuente.split()
                .branch(
                        (key, value) -> value.contains("\"language\":\"es\""),
                        Branched.withConsumer(
                                (consumer) -> consumer.to(topicCastellano,
                                Produced.with(Serdes.String(), Serdes.String()))
                        )
                )
                .branch(
                        (key, value) -> value.contains("\"language\":\"fr\""),
                        Branched.withConsumer(
                                (consumer) -> consumer.to(topicFrances,
                                Produced.with(Serdes.String(), Serdes.String()))
                        )
                )
                .branch(
                        (key, value) -> value.contains("\"language\":\"en\""),
                        Branched.withConsumer(
                                (consumer) -> consumer.to(topicIngles,
                                Produced.with(Serdes.String(), Serdes.String()))
                        )
                )
                .noDefaultBranch();
        log.info("Definición de Stream completada");
        
        // Ejecución del Stream
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
