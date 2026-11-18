package com.matty.integration.order.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("catalog.client")
public record CatalogClientProperties(String baseUrl, Duration connectTimeout, Duration readTimeout) {
}
