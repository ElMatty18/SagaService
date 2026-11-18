package com.matty.integration.order.client;

/**
 * Falla TRANSITORIA de infraestructura (5xx, timeout, conexión rechazada).
 * Es la única familia de errores que se reintenta y que cuenta para abrir el circuito.
 */
public class CatalogUnavailableException extends RuntimeException {

    public CatalogUnavailableException(String message) {
        super(message);
    }

    public CatalogUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
