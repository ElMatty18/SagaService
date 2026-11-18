package com.matty.integration.order.api;

import com.matty.integration.order.client.UnknownProductException;
import com.matty.integration.order.idempotency.IdempotencyService.IdempotencyKeyReusedException;
import com.matty.integration.order.quote.QuoteExceptions.InsufficientStockException;
import com.matty.integration.order.quote.QuoteExceptions.MixedCurrenciesException;
import com.matty.integration.order.quote.QuoteExceptions.PricingUnavailableException;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Traducción de errores internos a respuestas HTTP (RFC 7807) para NUESTROS clientes.
 *
 * Regla de integración: nunca filtrar el error del proveedor tal cual. Un 500 del catálogo
 * no debe aparecer como 500 nuestro: es un 503 "dependencia no disponible, reintentá"
 * con Retry-After. Los errores de negocio son 4xx (no reintentar).
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /** Las fallas de un CompletableFuture llegan envueltas: desenvolver y re-clasificar. */
    @ExceptionHandler({CompletionException.class, ExecutionException.class})
    ResponseEntity<ProblemDetail> wrapped(Exception ex) {
        Throwable cause = ex;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return toResponse(cause);
    }

    @ExceptionHandler({UnknownProductException.class, InsufficientStockException.class, PricingUnavailableException.class,
            MixedCurrenciesException.class, IdempotencyKeyReusedException.class, TimeoutException.class,
            ConstraintViolationException.class})
    ResponseEntity<ProblemDetail> known(Exception ex) {
        return toResponse(ex);
    }

    private ResponseEntity<ProblemDetail> toResponse(Throwable ex) {
        return switch (ex) {
            case UnknownProductException e -> problem(HttpStatus.UNPROCESSABLE_ENTITY, "unknown-product", e.getMessage(), null);
            case InsufficientStockException e -> problem(HttpStatus.CONFLICT, "insufficient-stock", e.getMessage(), null);
            case MixedCurrenciesException e -> problem(HttpStatus.UNPROCESSABLE_ENTITY, "mixed-currencies", e.getMessage(), null);
            case IdempotencyKeyReusedException e -> problem(HttpStatus.UNPROCESSABLE_ENTITY, "idempotency-key-reused", e.getMessage(), null);
            case ConstraintViolationException e -> problem(HttpStatus.BAD_REQUEST, "invalid-request", e.getMessage(), null);
            case PricingUnavailableException e -> problem(HttpStatus.SERVICE_UNAVAILABLE, "catalog-unavailable", e.getMessage(), "5");
            case TimeoutException e -> problem(HttpStatus.SERVICE_UNAVAILABLE, "quote-timeout",
                    "No se pudo cotizar el pedido dentro del tiempo máximo", "2");
            default -> {
                log.error("Error no controlado", ex);
                yield problem(HttpStatus.INTERNAL_SERVER_ERROR, "internal", "Error interno", null);
            }
        };
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String type, String detail, String retryAfter) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create("urn:problem:orders:" + type));
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(status);
        if (retryAfter != null) {
            builder.header(HttpHeaders.RETRY_AFTER, retryAfter);
        }
        return builder.body(problem);
    }
}
