package com.laboratorio.demoadminapi;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DemoAdminAPI {
    private static final Logger log = LogManager.getLogger(DemoAdminAPI.class);

    public static void main(String[] args) {
        log.info("Iniciando Demo Admin API");
        
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.69:9092");
        
        try (AdminClient adminClient = AdminClient.create(properties) ) {
            
            // Obtener información del cluster
            try {
                DescribeClusterResult clusterInfo = adminClient.describeCluster();

                String clusterId = clusterInfo.clusterId().get();
                log.info("Cluster Id: {}", clusterId);
                
                for (Node nodo : clusterInfo.nodes().get()) {
                    log.info("Broker: {}, Host: {}, Puerto: {}", nodo.id(), nodo.host(), nodo.port());
                }
                
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error recuperando la información del cluster: {}", e.getMessage());
            }
            
            // Crear un topic
            try {
                NewTopic newTopic = new NewTopic("admin-topic", 1, (short)1);
                adminClient.createTopics(Collections.singletonList(newTopic))
                        .all().get();
                
                log.info("Se ha creado el topic admin-topic");
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error creando el topic admin-topic: {}", e.getMessage());
                adminClient.close();
                log.info("Finalizando Demo Admin API");
                System.exit(-1);
            }
            
            // Listar topics existentes
            try {
                Set<String> topics = adminClient.listTopics()
                        .names().get();
                for (String topic : topics) {
                    log.info("Nombre del topic: {}", topic);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error al listar los topics del cluster: {}", e.getMessage());
            }
            
            // Obtener información de un topic
            try {
                adminClient.describeTopics(Collections.singletonList("admin-topic"))
                        .allTopicNames().get()
                        .forEach((name, description) ->{
                            log.info("Topic: {} - Particiones: {}", name, description.partitions().size());
                        });
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error consultado la información del topic admin-topic: {}", e.getMessage());
            }
            
            // Cambiar configuración de topic
            try {
                ConfigEntry entry = new ConfigEntry("retention.ms", "3600000");
                AlterConfigOp alterConfigOp = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
                
                adminClient.incrementalAlterConfigs(
                    Collections.singletonMap(
                            new ConfigResource(ConfigResource.Type.TOPIC, "admin-topic"),
                            Collections.singletonList(alterConfigOp)
                    )
                ).all().get();
                
                log.info("Se ha cambiado la configuración del topic admin-topic");
            } catch (InterruptedException | ExecutionException e) {
                log.error("No se ha logrado cambiar la configuración del topic admin-topic: {}", e.getMessage());
            }
            
            // Eliminar un topic
            try {
                adminClient.deleteTopics(Collections.singletonList("admin-topic"))
                        .all().get();
                
                log.info("Se ha eliminado el topic admin-topic");
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error eliminado el topic admin-topic: {}", e.getMessage());
            }
        
            adminClient.close();
        }
        
        log.info("Finalizando Demo Admin API");
    }
}