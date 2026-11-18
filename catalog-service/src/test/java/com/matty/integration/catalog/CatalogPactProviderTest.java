package com.matty.integration.catalog;

import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import au.com.dius.pact.provider.spring.junit5.MockMvcTestTarget;
import com.matty.integration.catalog.api.ApiExceptionHandler;
import com.matty.integration.catalog.api.DiscountController;
import com.matty.integration.catalog.api.ProductController;
import com.matty.integration.catalog.domain.CatalogQueryService;
import com.matty.integration.catalog.domain.Product;
import com.matty.integration.catalog.domain.ProductNotFoundException;
import com.matty.integration.catalog.domain.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * PROVIDER VERIFICATION (Pact).
 *
 * Reproduce cada interacción del contrato generado por order-service contra los controllers
 * REALES del catálogo (MockMvc, sin levantar BD ni Kafka). Los "provider states" preparan
 * los datos que cada interacción asume ("product SKU-001 exists", etc.).
 *
 * Si alguien renombra un campo de PriceResponse o cambia un status code que el consumidor
 * usa, este test falla en el build del catálogo.
 */
@Provider("catalog-service")
@PactFolder("../contracts/pacts")
class CatalogPactProviderTest {

    private final CatalogQueryService catalog = mock(CatalogQueryService.class);
    private final ProductRepository products = mock(ProductRepository.class);

    @BeforeEach
    void setUp(PactVerificationContext context) {
        MockMvcTestTarget target = new MockMvcTestTarget();
        target.setControllers(new ProductController(catalog, products), new DiscountController(catalog));
        target.setControllerAdvices(new ApiExceptionHandler());
        context.setTarget(target);
    }

    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void verifyInteraction(PactVerificationContext context) {
        context.verifyInteraction();
    }

    @State("product SKU-001 exists")
    void productExists() {
        when(catalog.getProduct("SKU-001"))
                .thenReturn(new Product("SKU-001", "Teclado", "ELECTRONICS", new BigDecimal("120.00"), "USD", 50));
    }

    @State("product SKU-404 does not exist")
    void productDoesNotExist() {
        when(catalog.getProduct("SKU-404")).thenThrow(new ProductNotFoundException("SKU-404"));
    }

    @State("a volume discount exists for ELECTRONICS")
    void discountExists() {
        when(catalog.getDiscountPercentage("ELECTRONICS", 3)).thenReturn(new BigDecimal("5.00"));
    }
}
