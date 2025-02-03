package com.laboratorio.demoproducerapi.modelo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter @AllArgsConstructor
public class Subscription {
    private String type;
    private String stream;
}