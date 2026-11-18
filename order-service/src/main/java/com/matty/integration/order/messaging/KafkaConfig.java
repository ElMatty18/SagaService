package com.matty.integration.order.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic orderCreatedTopic() {
        return TopicBuilder.name(Topics.ORDER_CREATED).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic stockReservedDlt() {
        return TopicBuilder.name(Topics.STOCK_RESERVED + Topics.DLT_SUFFIX).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic stockRejectedDlt() {
        return TopicBuilder.name(Topics.STOCK_REJECTED + Topics.DLT_SUFFIX).partitions(3).replicas(1).build();
    }

    /**
     * REINTENTOS BLOQUEANTES + DLT (estrategia opuesta a la de catalog-service).
     *
     * El consumidor reintenta EN EL MISMO hilo, con backoff exponencial (0.5s, 1s, 2s),
     * sin avanzar el offset. Si se agotan los intentos, publica el mensaje en <topic>-dlt
     * (misma partición, para conservar el orden relativo) y sigue con el siguiente.
     *
     * ¿Por qué bloqueante acá? Los eventos de un mismo pedido deben aplicarse EN ORDEN.
     * Con retry topics un evento podría "adelantarse" a otro que está esperando en el retry.
     * El costo: un mensaje problemático frena su partición mientras dura el backoff.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + Topics.DLT_SUFFIX, record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // Poison pills: reintentar un JSON inválido nunca va a funcionar.
        handler.addNotRetryableExceptions(NonRetryableEventException.class);
        return handler;
    }
}
