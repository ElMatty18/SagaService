package com.matty.integration.catalog.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * Lecturas que expone la API REST. La simulación de fallas se aplica acá
 * (y no en el controller) para que afecte exactamente lo mismo que vería un cliente real.
 */
@Service
@Transactional(readOnly = true)
public class CatalogQueryService {

    private final ProductRepository products;
    private final DiscountRepository discounts;
    private final FaultSimulator faults;

    public CatalogQueryService(ProductRepository products, DiscountRepository discounts, FaultSimulator faults) {
        this.products = products;
        this.discounts = discounts;
        this.faults = faults;
    }

    public Product getProduct(String sku) {
        faults.maybeFail("product");
        return products.findById(sku).orElseThrow(() -> new ProductNotFoundException(sku));
    }

    public BigDecimal getDiscountPercentage(String category, int quantity) {
        faults.maybeDelay("discount");
        return discounts.findTopByCategoryAndMinQuantityLessThanEqualOrderByMinQuantityDesc(category, quantity)
                .map(Discount::getPercentage)
                .orElse(BigDecimal.ZERO);
    }
}
