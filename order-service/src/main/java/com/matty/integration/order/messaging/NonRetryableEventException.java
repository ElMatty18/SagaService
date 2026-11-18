package com.matty.integration.order.messaging;

/**
 * Error permanente: el mensaje es inválido (JSON roto, campos faltantes).
 * Reintentarlo no sirve de nada, va directo al DLT ("poison pill").
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
