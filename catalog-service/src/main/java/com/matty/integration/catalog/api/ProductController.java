package com.matty.integration.catalog.api;

import com.matty.integration.catalog.api.CatalogDtos.PriceResponse;
import com.matty.integration.catalog.api.CatalogDtos.ProductSummary;
import com.matty.integration.catalog.api.CatalogDtos.StockResponse;
import com.matty.integration.catalog.domain.CatalogQueryService;
import com.matty.integration.catalog.domain.Product;
import com.matty.integration.catalog.domain.ProductRepository;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;

@RestController
@RequestMapping("/api/v1/products")
public class ProductController {

    private final CatalogQueryService catalog;
    private final ProductRepository products;

    public ProductController(CatalogQueryService catalog, ProductRepository products) {
        this.catalog = catalog;
        this.products = products;
    }

    @GetMapping
    public List<ProductSummary> list() {
        return products.findAll().stream()
                .map(p -> new ProductSummary(p.getSku(), p.getName(), p.getCategory(), p.getUnitPrice(), p.getCurrency(), p.getStock()))
                .toList();
    }

    /** El stock cambia constantemente: no se cachea. */
    @GetMapping("/{sku}/stock")
    public ResponseEntity<StockResponse> stock(@PathVariable String sku) {
        Product product = catalog.getProduct(sku);
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(new StockResponse(product.getSku(), product.getStock()));
    }

    /** El precio es relativamente estable: se indica a los clientes que pueden cachearlo brevemente. */
    @GetMapping("/{sku}/price")
    public ResponseEntity<PriceResponse> price(@PathVariable String sku) {
        Product product = catalog.getProduct(sku);
        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(Duration.ofSeconds(30)))
                .body(new PriceResponse(product.getSku(), product.getUnitPrice(), product.getCurrency(), product.getCategory()));
    }
}
