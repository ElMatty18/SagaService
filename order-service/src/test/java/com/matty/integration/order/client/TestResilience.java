package com.matty.integration.order.client;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * Arma un CatalogClient real (HTTP + Resilience4j) sin Spring, con una política
 * equivalente a la de application.yml pero con tiempos cortos para que los tests sean rápidos.
 */
public final class TestResilience {

    public final CircuitBreakerRegistry circuitBreakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
            .slidingWindowSize(4)
            .minimumNumberOfCalls(4)
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .recordExceptions(CatalogUnavailableException.class, TimeoutException.class, BulkheadFullException.class)
            .ignoreExceptions(UnknownProductException.class, RequestNotPermitted.class)
            .build());

    public final RetryRegistry retries = RetryRegistry.of(RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(10))
            .retryExceptions(CatalogUnavailableException.class, TimeoutException.class)
            .ignoreExceptions(UnknownProductException.class, CallNotPermittedException.class,
                    RequestNotPermitted.class, BulkheadFullException.class)
            .build());

    public final TimeLimiterRegistry timeLimiters = TimeLimiterRegistry.of(TimeLimiterConfig.custom()
            .timeoutDuration(Duration.ofMillis(500)).build());

    public final ThreadPoolBulkheadRegistry bulkheads = ThreadPoolBulkheadRegistry.of(ThreadPoolBulkheadConfig.custom()
            .coreThreadPoolSize(4).maxThreadPoolSize(4).queueCapacity(10).build());

    public final RateLimiterRegistry rateLimiters = RateLimiterRegistry.of(RateLimiterConfig.custom()
            .limitForPeriod(1000).limitRefreshPeriod(Duration.ofSeconds(1)).timeoutDuration(Duration.ZERO).build());

    public final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public CatalogClient client(String baseUrl) {
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(300))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofMillis(300));
        RestClient restClient = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
        return new CatalogClient(restClient, new ResilientExecutor(circuitBreakers, retries, timeLimiters,
                bulkheads, rateLimiters, scheduler));
    }
}
