package com.matty.integration.order.quote;

/** Errores de negocio de la cotización. */
public final class QuoteExceptions {

    private QuoteExceptions() {
    }

    /** El catálogo informa stock insuficiente (validación temprana, antes del saga). */
    public static class InsufficientStockException extends RuntimeException {
        public InsufficientStockException(String sku, int requested, int available) {
            super("Stock insuficiente para " + sku + ": pedido " + requested + ", disponible " + available);
        }
    }

    /** No hay precio fresco NI cacheado: no podemos cotizar => 503. */
    public static class PricingUnavailableException extends RuntimeException {
        public PricingUnavailableException(String sku, Throwable cause) {
            super("No se pudo obtener el precio de " + sku + " y no hay un precio reciente en cache", cause);
        }
    }

    public static class MixedCurrenciesException extends RuntimeException {
        public MixedCurrenciesException() {
            super("El pedido mezcla productos con distintas monedas");
        }
    }
}
