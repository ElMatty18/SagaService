package com.matty.integration.catalog.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Cada servicio declara los topics que PRODUCE (es su dueño). */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic stockReservedTopic() {
        return TopicBuilder.name(Topics.STOCK_RESERVED).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic stockRejectedTopic() {
        return TopicBuilder.name(Topics.STOCK_REJECTED).partitions(3).replicas(1).build();
    }
}
