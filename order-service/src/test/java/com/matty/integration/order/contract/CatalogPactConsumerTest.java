package com.matty.integration.order.contract;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import com.matty.integration.order.client.CatalogClient;
import com.matty.integration.order.client.CatalogDtos.DiscountInfo;
import com.matty.integration.order.client.CatalogDtos.PriceInfo;
import com.matty.integration.order.client.CatalogDtos.StockInfo;
import com.matty.integration.order.client.TestResilience;
import com.matty.integration.order.client.UnknownProductException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CONSUMER-DRIVEN CONTRACT TEST (Pact).
 *
 * order-service declara QUÉ espera del catálogo (sólo lo que usa) y lo ejercita con su
 * cliente REAL contra un mock server de Pact. Esto genera contracts/pacts/order-service-catalog-service.json,
 * que catalog-service verifica en su build (CatalogPactProviderTest).
 *
 * Resultado: si el catálogo rompe algo que este consumidor usa, el build del CATÁLOGO falla,
 * antes del deploy. Los matchers (stringType, decimalType...) validan tipos, no valores exactos.
 */
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "catalog-service", pactVersion = PactSpecVersion.V3)
class CatalogPactConsumerTest {

    private static final Map<String, String> JSON = Map.of("Content-Type", "application/json");

    @Pact(consumer = "order-service")
    RequestResponsePact stockOfExistingProduct(PactDslWithProvider builder) {
        return builder
                .given("product SKU-001 exists")
                .uponReceiving("a stock request for an existing product")
                .path("/api/v1/products/SKU-001/stock").method("GET")
                .willRespondWith().status(200).headers(JSON)
                .body(new PactDslJsonBody().stringValue("sku", "SKU-001").integerType("available", 50))
                .toPact();
    }

    @Pact(consumer = "order-service")
    RequestResponsePact priceOfExistingProduct(PactDslWithProvider builder) {
        return builder
                .given("product SKU-001 exists")
                .uponReceiving("a price request for an existing product")
                .path("/api/v1/products/SKU-001/price").method("GET")
                .willRespondWith().status(200).headers(JSON)
                .body(new PactDslJsonBody()
                        .stringValue("sku", "SKU-001")
                        .decimalType("unitPrice", 120.00)
                        .stringMatcher("currency", "[A-Z]{3}", "USD")
                        .stringType("category", "ELECTRONICS"))
                .toPact();
    }

    @Pact(consumer = "order-service")
    RequestResponsePact discountForCategory(PactDslWithProvider builder) {
        return builder
                .given("a volume discount exists for ELECTRONICS")
                .uponReceiving("a discount request for a category and quantity")
                .path("/api/v1/discounts").method("GET")
                .query("category=ELECTRONICS&quantity=3")
                .willRespondWith().status(200).headers(JSON)
                .body(new PactDslJsonBody()
                        .stringValue("category", "ELECTRONICS")
                        .integerType("quantity", 3)
                        .decimalType("percentage", 5.00))
                .toPact();
    }

    @Pact(consumer = "order-service")
    RequestResponsePact unknownProduct(PactDslWithProvider builder) {
        return builder
                .given("product SKU-404 does not exist")
                .uponReceiving("a price request for an unknown product")
                .path("/api/v1/products/SKU-404/price").method("GET")
                .willRespondWith().status(404)
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "stockOfExistingProduct")
    void stock(MockServer server) {
        StockInfo stock = client(server).getStock("SKU-001").join();
        assertThat(stock.available()).isEqualTo(50);
    }

    @Test
    @PactTestFor(pactMethod = "priceOfExistingProduct")
    void price(MockServer server) {
        PriceInfo price = client(server).getPrice("SKU-001").join();
        assertThat(price.currency()).isEqualTo("USD");
        assertThat(price.category()).isEqualTo("ELECTRONICS");
    }

    @Test
    @PactTestFor(pactMethod = "discountForCategory")
    void discount(MockServer server) {
        DiscountInfo discount = client(server).getDiscount("ELECTRONICS", 3).join();
        assertThat(discount.percentage()).isEqualByComparingTo("5.00");
    }

    @Test
    @PactTestFor(pactMethod = "unknownProduct")
    void notFound(MockServer server) {
        assertThatThrownBy(() -> client(server).getPrice("SKU-404").join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(UnknownProductException.class);
    }

    private static CatalogClient client(MockServer server) {
        return new TestResilience().client(server.getUrl());
    }
}
