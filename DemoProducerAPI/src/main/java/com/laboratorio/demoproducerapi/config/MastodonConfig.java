package com.laboratorio.demoproducerapi.config;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MastodonConfig {
    private static final Logger log = LogManager.getLogger(MastodonConfig.class);
    private static MastodonConfig instance;
    private final Properties properties;

    private MastodonConfig() {
        this.properties = new Properties();
        this.loadProperties();
    }
    
    private void loadProperties() {
        try {
            this.properties.load(new FileReader("config//config.properties"));
        } catch (IOException e) {
            log.error("Error leyendo el fichero de configuración. Finaliza la aplicación");
            log.error("Error: " + e.getMessage());
            if (e.getCause() != null) {
                log.error("Causa: " + e.getCause().getMessage());
            }
            System.exit(-1);
        }
    }
    
    public static MastodonConfig getInstance() {
        if (instance == null) {
            synchronized (MastodonConfig.class) {
                if (instance == null) {
                    log.info("Se ha creado la instancia de configuración");
                    instance = new MastodonConfig();
                }
            }
        } else {
            log.info("La instancia de configuración ya existe");
        }
        
        return instance;
    }
    
    public String getProperty(String key) {
        return this.properties.getProperty(key);
    }
}