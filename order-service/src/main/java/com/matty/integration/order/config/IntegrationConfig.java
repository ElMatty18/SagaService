package com.matty.integration.order.config;

import com.matty.integration.order.client.CatalogClient;
import com.matty.integration.order.client.CatalogClientProperties;
import com.matty.integration.order.client.ResilientExecutor;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.micrometer.context.ContextRegistry;
import io.micrometer.context.integration.Slf4jThreadLocalAccessor;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.matty.integration.order.config.CorrelationIdFilter.HEADER;
import static com.matty.integration.order.config.CorrelationIdFilter.MDC_KEY;

@Configuration
public class IntegrationConfig {

    static {
        // Hace que ContextSnapshot.captureAll() incluya también el correlationId del MDC
        // (el traceId de OpenTelemetry ya se propaga vía ObservationThreadLocalAccessor).
        ContextRegistry.getInstance().registerThreadLocalAccessor(new Slf4jThreadLocalAccessor(MDC_KEY));
    }

    /**
     * RestClient hacia el catálogo con timeouts explícitos.
     * Se parte del RestClient.Builder de Spring Boot porque viene instrumentado:
     * genera métricas http.client.requests y propaga el header W3C "traceparent".
     */
    @Bean
    RestClient catalogRestClient(RestClient.Builder builder, CatalogClientProperties props) {
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(props.connectTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(props.readTimeout());

        return builder
                .baseUrl(props.baseUrl())
                .requestFactory(requestFactory)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .requestInterceptor((request, body, execution) -> {
                    String correlationId = MDC.get(MDC_KEY);
                    if (correlationId != null) {
                        request.getHeaders().set(HEADER, correlationId);
                    }
                    return execution.execute(request, body);
                })
                .build();
    }

    /**
     * El scheduler de Resilience4j (backoff de Retry y timeouts de TimeLimiter) es PRIVADO del
     * ResilientExecutor y NO se publica como bean: si hubiera un bean ScheduledExecutorService,
     * Spring Boot lo adoptaría también para los @Scheduled (p. ej. el OutboxRelay), acoplando
     * dos cosas que no tienen nada que ver.
     */
    @Bean(destroyMethod = "shutdown")
    ResilientExecutor resilientExecutor(CircuitBreakerRegistry cb, RetryRegistry retry, TimeLimiterRegistry tl,
                                        ThreadPoolBulkheadRegistry bh, RateLimiterRegistry rl) {
        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(4, Thread.ofPlatform().name("resilience-", 0).daemon().factory());
        return new ResilientExecutor(cb, retry, tl, bh, rl, scheduler);
    }

    @Bean
    CatalogClient catalogClient(RestClient catalogRestClient, ResilientExecutor resilientExecutor) {
        return new CatalogClient(catalogRestClient, resilientExecutor);
    }

    /**
     * Executor de la APLICACIÓN para continuar los CompletableFuture (persistir el pedido, etc.).
     * Virtual threads: baratos para trabajo bloqueante (JDBC).
     * El contexto (traceId/correlationId) se propaga explícitamente en OrderController.
     */
    @Bean(destroyMethod = "shutdown")
    ExecutorService orderExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
