package com.matty.integration.catalog.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

@Configuration
public class ObservabilityConfig {

    /**
     * Filtra observaciones que sólo generan ruido en Jaeger:
     *  - scrapes de Prometheus y health checks (/actuator/**)
     *  - la ejecución periódica del OutboxRelay (cada 500ms): el relay crea su propio span,
     *    hijo de la traza original, sólo cuando realmente publica algo.
     */
    @Bean
    ObservationPredicate ignoreNoise() {
        return (name, context) -> {
            if (name.equals("tasks.scheduled.execution")) {
                return false;
            }
            if (context instanceof ServerRequestObservationContext server) {
                return !server.getCarrier().getRequestURI().startsWith("/actuator");
            }
            return true;
        };
    }
}
