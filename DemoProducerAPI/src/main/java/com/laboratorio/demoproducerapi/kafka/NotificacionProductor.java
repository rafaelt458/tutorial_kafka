package com.laboratorio.demoproducerapi.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NotificacionProductor implements Callback {
    private static final Logger log = LogManager.getLogger(NotificacionProductor.class);
    
    @Override
    public void onCompletion(RecordMetadata rm, Exception excptn) {
        if (excptn == null) {
            log.info("El mensaje {} fue enviado a la partición {} del topic {}", rm.offset(), rm.partition(), rm.topic());
        } else {
            log.error("Error enviando el mensaje: " + excptn.getMessage());
        }
    }    
}