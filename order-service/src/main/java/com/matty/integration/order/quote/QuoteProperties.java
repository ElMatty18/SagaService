package com.matty.integration.order.quote;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("order.quote")
public record QuoteProperties(Duration totalBudget, Duration discountBudget, Duration priceCacheTtl) {
}
