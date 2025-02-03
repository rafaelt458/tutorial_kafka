package com.laboratorio.demoproducerapi.modelo;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class Event {
    private List<String> stream;
    private String event;
    private String payload;
}