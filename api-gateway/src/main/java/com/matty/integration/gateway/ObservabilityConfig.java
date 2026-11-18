package com.matty.integration.gateway;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;

@Configuration
public class ObservabilityConfig {

    /** Sin trazas para scrapes de Prometheus ni health checks. */
    @Bean
    ObservationPredicate ignoreActuator() {
        return (name, context) -> !(context instanceof ServerRequestObservationContext server
                && server.getCarrier().getPath().value().startsWith("/actuator"));
    }
}
