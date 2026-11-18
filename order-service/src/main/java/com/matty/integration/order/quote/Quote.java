package com.matty.integration.order.quote;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/** Resultado de cotizar un pedido. Inmutable. */
public record Quote(List<Line> lines, BigDecimal total, String currency, List<String> warnings) {

    public record Line(String sku, int quantity, BigDecimal unitPrice, BigDecimal discountPercentage,
                       BigDecimal lineTotal, List<String> warnings) {

        static Line of(String sku, int quantity, BigDecimal unitPrice, BigDecimal discountPercentage, List<String> warnings) {
            BigDecimal factor = BigDecimal.ONE.subtract(discountPercentage.movePointLeft(2));
            BigDecimal total = unitPrice.multiply(BigDecimal.valueOf(quantity)).multiply(factor)
                    .setScale(2, RoundingMode.HALF_EVEN);
            return new Line(sku, quantity, unitPrice, discountPercentage, total, List.copyOf(warnings));
        }
    }

    public static Quote of(List<Line> lines, String currency) {
        BigDecimal total = lines.stream().map(Line::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add);
        List<String> warnings = lines.stream().flatMap(l -> l.warnings().stream()).toList();
        return new Quote(List.copyOf(lines), total, currency, warnings);
    }

    /** true si algún dato vino de un fallback (precio cacheado, descuento omitido). */
    public boolean degraded() {
        return !warnings.isEmpty();
    }
}
