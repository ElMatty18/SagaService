package com.matty.integration.catalog.api;

import java.math.BigDecimal;

/**
 * DTOs de la API pública del catálogo. Son el CONTRATO con los consumidores:
 * cualquier cambio acá debe pasar la verificación Pact (CatalogPactProviderTest).
 * Regla: agregar campos es compatible; renombrar/eliminar NO (se versiona la ruta: /api/v2).
 */
public final class CatalogDtos {

    private CatalogDtos() {
    }

    public record StockResponse(String sku, int available) {
    }

    public record PriceResponse(String sku, BigDecimal unitPrice, String currency, String category) {
    }

    public record DiscountResponse(String category, int quantity, BigDecimal percentage) {
    }

    public record ProductSummary(String sku, String name, String category, BigDecimal unitPrice, String currency, int stock) {
    }
}
