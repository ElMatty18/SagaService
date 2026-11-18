package com.matty.integration.order.client;

import java.math.BigDecimal;

/**
 * Vista del CONSUMIDOR sobre la API del catálogo: sólo los campos que usamos.
 * Si el catálogo agrega campos no nos rompe (tolerant reader); si quita uno que
 * usamos, lo detecta el contrato Pact antes de llegar a producción.
 */
public final class CatalogDtos {

    private CatalogDtos() {
    }

    public record StockInfo(String sku, int available) {
    }

    public record PriceInfo(String sku, BigDecimal unitPrice, String currency, String category) {
    }

    public record DiscountInfo(String category, int quantity, BigDecimal percentage) {
    }
}
