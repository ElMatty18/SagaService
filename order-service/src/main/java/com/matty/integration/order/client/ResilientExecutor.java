package com.matty.integration.order.client;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.micrometer.context.ContextSnapshot;
import io.micrometer.context.ContextSnapshotFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * NÚCLEO DE LA RESILIENCIA: compone los 5 patrones de Resilience4j alrededor de una
 * llamada bloqueante y devuelve un CompletableFuture.
 *
 * Se compone programáticamente (en vez de con anotaciones @CircuitBreaker, @Retry...) para
 * que el ORDEN sea explícito y visible. De adentro hacia afuera:
 *
 *   Retry                      reintenta el conjunto completo con backoff exponencial + jitter
 *    └─ CircuitBreaker         si el catálogo viene fallando, corta en seco (CallNotPermittedException)
 *        └─ RateLimiter        no superar N llamadas/seg hacia el catálogo
 *            └─ TimeLimiter    cada intento tiene un tiempo máximo
 *                └─ Bulkhead   ejecuta en un pool de threads acotado y AISLADO por operación
 *                    └─ HTTP   (con connect/read timeout propios)
 *
 * Por qué este orden (mismo que usa Spring con las anotaciones):
 *  - Retry afuera de todo: cada reintento vuelve a pasar por el circuito, así que cuando
 *    el circuito se abre los reintentos se cortan de inmediato (no hay retry storm).
 *  - CircuitBreaker afuera de TimeLimiter: un timeout CUENTA como falla para el circuito.
 *  - Bulkhead adentro: el permiso de concurrencia se toma sólo durante la llamada real.
 *
 * Instancias por nombre ("catalog-price", ...) configuradas en application.yml; al
 * obtenerlas de los registries de Spring Boot quedan expuestas en /actuator y Prometheus.
 */
public class ResilientExecutor {

    private final CircuitBreakerRegistry circuitBreakers;
    private final RetryRegistry retries;
    private final TimeLimiterRegistry timeLimiters;
    private final ThreadPoolBulkheadRegistry bulkheads;
    private final RateLimiterRegistry rateLimiters;
    /** Para esperas de backoff y timeouts: nunca bloquear un thread durmiendo. */
    private final ScheduledExecutorService scheduler;
    private final ContextSnapshotFactory snapshots = ContextSnapshotFactory.builder().build();

    public ResilientExecutor(CircuitBreakerRegistry circuitBreakers, RetryRegistry retries,
                             TimeLimiterRegistry timeLimiters, ThreadPoolBulkheadRegistry bulkheads,
                             RateLimiterRegistry rateLimiters, ScheduledExecutorService scheduler) {
        this.circuitBreakers = circuitBreakers;
        this.retries = retries;
        this.timeLimiters = timeLimiters;
        this.bulkheads = bulkheads;
        this.rateLimiters = rateLimiters;
        this.scheduler = scheduler;
    }

    public <T> CompletableFuture<T> execute(String name, Supplier<T> blockingCall) {
        ThreadPoolBulkhead bulkhead = bulkheads.bulkhead(name);
        TimeLimiter timeLimiter = timeLimiters.timeLimiter(name);
        RateLimiter rateLimiter = rateLimiters.rateLimiter(name);
        CircuitBreaker circuitBreaker = circuitBreakers.circuitBreaker(name);
        Retry retry = retries.retry(name);

        // La llamada corre en OTRO thread (el del bulkhead): hay que llevar el contexto
        // (traceId de OpenTelemetry + correlationId del MDC) o se pierde la trazabilidad.
        ContextSnapshot snapshot = snapshots.captureAll();
        Supplier<T> withContext = () -> {
            try (ContextSnapshot.Scope ignored = snapshot.setThreadLocals()) {
                return blockingCall.get();
            }
        };

        Supplier<CompletionStage<T>> isolated = () -> {
            try {
                return bulkhead.executeSupplier(withContext);
            } catch (BulkheadFullException e) {
                // Pool y cola llenos: el catálogo no está dando abasto. Fallar YA, no encolar más.
                return CompletableFuture.failedFuture(e);
            }
        };
        Supplier<CompletionStage<T>> timed = () -> timeLimiter.executeCompletionStage(scheduler, isolated);
        Supplier<CompletionStage<T>> throttled = RateLimiter.decorateCompletionStage(rateLimiter, timed);
        Supplier<CompletionStage<T>> guarded = CircuitBreaker.decorateCompletionStage(circuitBreaker, throttled);
        Supplier<CompletionStage<T>> retried = Retry.decorateCompletionStage(retry, scheduler, guarded);

        // El future se COMPLETA dentro del contexto capturado. Las etapas que el caller encadene
        // (thenCompose, thenApply...) corren sincrónicamente al completarse, así que heredan
        // traceId/correlationId aunque el thread que completa sea uno del bulkhead o del scheduler.
        // Sin esto, p. ej. la llamada a "discount" (encadenada con thenCompose tras "price")
        // arrancaría una traza nueva.
        CompletableFuture<T> result = new CompletableFuture<>();
        retried.get().whenComplete((value, error) -> {
            try (ContextSnapshot.Scope ignored = snapshot.setThreadLocals()) {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(value);
                }
            }
        });
        return result;
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
