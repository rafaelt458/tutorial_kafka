package com.laboratorio.demoproducerapi;

import com.google.gson.Gson;
import com.laboratorio.demoproducerapi.config.MastodonConfig;
import com.laboratorio.demoproducerapi.kafka.ProductorKafka;
import com.laboratorio.demoproducerapi.modelo.Event;
import com.laboratorio.demoproducerapi.modelo.Subscription;
import java.net.URI;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;


public class DemoProducerApi {
    private static final Logger log = LogManager.getLogger(DemoProducerApi.class);
    private static final Gson gson = new Gson();
    private static ProductorKafka productorKafka = null;
    
    public static void main(String[] args) {
        try {
            MastodonConfig config = MastodonConfig.getInstance();
            URI uri = new URI(config.getProperty("uri_mastodon"));
            String token = config.getProperty("mastodon_token");
            
            Map<String, String> headers = Map.of("Authorization", "Bearer " + token);
            WebSocketClient client = new WebSocketClient(uri, headers) {
                @Override
                public void onOpen(ServerHandshake sh) {
                    log.info("Se ha abierto la conexión: " + sh.getHttpStatusMessage());
                    
                    Subscription subscription = new Subscription("subscribe", "public");
                    String json = gson.toJson(subscription);
                    log.info("Creando la subscripción: " + json);
                    send(json);
                    
                    String cluster = "192.168.1.69:9092";
                    String topic = "mastodon_topic";
                    productorKafka = new ProductorKafka(cluster, topic);
                }
                
                @Override
                public void onMessage(String mensaje) {
                    log.info("Se recibió el mensaje: " + mensaje);
                    
                    procesarMensaje(mensaje);
                }
                
                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.info("Se ha cerrado la conexión. Motivo: " + reason);
                    if (productorKafka != null) {
                        productorKafka.close();
                    }
                }
                
                @Override
                public void onError(Exception ex) {
                    log.error(("Ha ocurrido el siguente error: " + ex.getMessage()));
                }
            };
            
            client.connect();
        } catch (Exception e) {
            log.error("Error procesando el Stream de Mastodon: " + e.getMessage());
        }
    }
    
    private static void procesarMensaje(String mensaje) {
        Event event = gson.fromJson(mensaje, Event.class);
        if (event.getEvent().equals("update")) {
            // log.info("Procesar el payload: " + event.getPayload());
            if (productorKafka != null) {
                productorKafka.send(event.getPayload());
            }
        }
    }
}
