package com.matty.integration.catalog.api;

import com.matty.integration.catalog.api.CatalogDtos.DiscountResponse;
import com.matty.integration.catalog.domain.CatalogQueryService;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/api/v1/discounts")
public class DiscountController {

    private final CatalogQueryService catalog;

    public DiscountController(CatalogQueryService catalog) {
        this.catalog = catalog;
    }

    @GetMapping
    public DiscountResponse discount(@RequestParam @NotBlank String category,
                                     @RequestParam @Min(1) int quantity) {
        return new DiscountResponse(category, quantity, catalog.getDiscountPercentage(category, quantity));
    }
}
