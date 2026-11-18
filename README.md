# Ejercicio de integración entre microservicios

Dos servicios simples (pedidos y catálogo) cuya **complejidad está en cómo se comunican**:
resiliencia síncrona, mensajería asíncrona confiable, límites de uso, contratos y observabilidad.

```
                    X-API-Key
  cliente ──────────────────────▶ api-gateway :8080  (Spring Cloud Gateway)
                                   │ auth por API key · rate limit por plan (Redis)
                                   │ circuit breaker de borde · correlation id
                                   ▼
                              order-service :8081
                                   │  CompletableFuture: stock ∥ (price → discount), N líneas en paralelo
                                   │  Resilience4j: Retry · CircuitBreaker · RateLimiter · TimeLimiter · Bulkhead
                                   │
                  REST ────────────┼──▶ toxiproxy :8666 ──▶ catalog-service :8082
                                   │      (chaos)            load shedding (429)
                                   │
                  Kafka ───────────┴── outbox ──▶ orders.order-created.v1 ──▶ catalog (reserva stock)
                  (saga)                            catalog.stock-reserved.v1 / stock-rejected.v1 ◀── outbox
                                                    retry topics + DLT · consumidores idempotentes

  orders-db / catalog-db (Postgres, uno por servicio) · Redis · Kafka KRaft
  Prometheus + Grafana · Jaeger (OpenTelemetry) · Kafka UI
```

## Levantar

Requisitos: Docker (con Compose v2). Para correr los tests también Java 21 y Maven.

```bash
make up        # construye y levanta los 12 contenedores, espera a que estén healthy
make demo      # corre los 7 escenarios de demo
make test      # unitarios + contratos Pact + integración con Testcontainers
make help      # todos los comandos
```

| URL | Qué es |
|---|---|
| http://localhost:8080 | API Gateway (único punto de entrada) |
| http://localhost:3000 | Grafana: dashboard "Integración: resiliencia y saga" |
| http://localhost:16686 | Jaeger: trazas distribuidas |
| http://localhost:8090 | Kafka UI: topics, retry topics y DLT |
| http://localhost:8081/actuator/circuitbreakers | Estado de los circuitos |

API keys de demo: `demo-free-key` (2 req/s, ráfaga 5) y `demo-premium-key` (20 req/s, ráfaga 40).

```bash
curl -i -X POST localhost:8080/api/v1/orders \
  -H 'X-API-Key: demo-premium-key' \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"ana","items":[{"sku":"SKU-001","quantity":3},{"sku":"SKU-004","quantity":2}]}'
# 202 Accepted + Location: /api/v1/orders/{id}   (PENDING → CONFIRMED/REJECTED vía Kafka)
```

Productos semilla: `SKU-001`…`SKU-005` (`SKU-003` tiene stock 5 y `SKU-005` tiene stock 1, útiles para ver rechazos).

## Mapa de patrones

### Comunicación síncrona (order → catalog)

| Patrón | Dónde | Detalle |
|---|---|---|
| Composición de los 5 patrones | `order-service/.../client/ResilientExecutor.java` | `Retry(CircuitBreaker(RateLimiter(TimeLimiter(Bulkhead(http)))))`, orden explicado en el código |
| Circuit breaker **por operación** | `application.yml` → `resilience4j.circuitbreaker` | `catalog-stock`, `catalog-price` y `catalog-discount` se abren por separado |
| Retry con backoff exponencial + jitter | idem `resilience4j.retry` | Sólo errores transitorios; nunca 4xx, circuito abierto ni bulkhead lleno |
| Timeouts en 3 niveles | `catalog.client.*`, `timelimiter`, `order.quote.total-budget` | HTTP (800ms) → intento (1s) → presupuesto total del pedido (2.5s) |
| Bulkhead (thread pool aislado) | `thread-pool-bulkhead` | Un endpoint lento sólo agota *su* pool |
| Rate limit del cliente | `ratelimiter` | El consumidor no le manda al catálogo más de lo acordado |
| Load shedding del proveedor | `catalog-service/.../config/ServerRateLimitFilter.java` | 429 + `Retry-After` en vez de degradar a todos |
| Clasificación de errores | `CatalogClient.throwFor` | 404 = negocio (sin retry y sin abrir el circuito); 5xx/429/IO = transitorio |
| Fallbacks según criticidad | `quote/OrderQuoteService.java` | Ver tabla en el Javadoc |
| Errores RFC 7807 | `ApiExceptionHandler` (ambos servicios) | La falla del proveedor se traduce: un 500 del catálogo llega al cliente como 503 + `Retry-After` |

### `CompletableFuture` (order-service)

`OrderQuoteService` cotiza cada línea con este grafo:

```
        ┌──────── stock(sku) ─────────────────────────┐
   sku ─┤                                              ├─ thenCombine ─▶ línea cotizada
        └─ price(sku) ──thenCompose──▶ discount(cat) ─┘
```

| Operador | Uso |
|---|---|
| `thenCompose` | `discount` necesita la categoría que devuelve `price` (llamada dependiente) |
| `thenCombine` | une `stock` y `price+discount`, que corren en paralelo |
| `allOf` → `allOfFailFast` | fan-in de N líneas que falla apenas falla una |
| `orTimeout` | presupuesto global: **falla** con `TimeoutException` → 503 |
| `completeOnTimeout` | descuento opcional: **degrada** a 0% si no llega en 400ms |
| `exceptionallyCompose` | precio: fallback asíncrono al último precio conocido |
| `handle` | stock: convierte la falla en "no verificado" (la reserva por saga es la verdad) |
| `thenApplyAsync(executor)` | persistir en un executor propio, nunca en los threads del bulkhead |

`OrderController` devuelve el `CompletableFuture` directamente (async servlet): el thread del request se libera mientras se cotiza.

### Comunicación asíncrona (saga por coreografía)

| Patrón | Dónde |
|---|---|
| Transactional outbox | `outbox/OutboxWriter.java` (`Propagation.MANDATORY`) + `outbox/OutboxRelay.java` (`FOR UPDATE SKIP LOCKED`) |
| Consumidor idempotente | `messaging/ProcessedEventStore.java` (`INSERT … ON CONFLICT DO NOTHING` en la misma transacción) |
| Retry **no bloqueante** + DLT | `catalog-service/.../OrderEventsListener.java` (`@RetryableTopic`: `-retry-0..2`, `-dlt`) |
| Retry **bloqueante** + DLT | `order-service/.../messaging/KafkaConfig.java` (`DefaultErrorHandler`, conserva el orden) |
| Poison pills | `NonRetryableEventException` → directo al DLT |
| Reserva todo-o-nada sin deadlocks | `StockReservationService` (locks ordenados por SKU) |
| Máquina de estados tolerante a duplicados/desorden | `domain/Order.confirm()/reject()` + `@Version` |
| Versionado de topics | `<dueño>.<evento>.v1`, esquemas locales por servicio (tolerant reader) |

### Edge (api-gateway)

| Patrón | Dónde |
|---|---|
| Autenticación por API key; la key no viaja a los servicios internos | `ApiKeyAuthFilter.java` |
| Rate limit por plan, distribuido (token bucket en Redis), con fail-open | `PlanAwareRateLimiter.java` |
| Circuit breaker de borde + fallback 503 | `RoutesConfig.java`, `FallbackController.java` |
| Retry sólo en GET; límite de tamaño de request | `RoutesConfig.java` |
| Correlation id | `CorrelationIdFilter.java` → header HTTP → MDC → outbox → header Kafka |

### Idempotencia del POST

Header `Idempotency-Key` obligatorio (`idempotency/IdempotencyService.java`):
- misma key y mismo body: devuelve el mismo pedido con `Idempotent-Replayed: true`, sin volver a llamar al catálogo;
- misma key con otro body: 422;
- dos requests concurrentes con la misma key: la PK de `idempotency_keys` deja pasar a una sola.

## Escenarios de caos

`order-service` no habla directo con el catálogo: pasa por **Toxiproxy**, que permite romper la red en caliente.

```bash
./scripts/chaos.sh latency 1500   # latencia en la red
./scripts/chaos.sh timeout        # las respuestas nunca llegan
./scripts/chaos.sh down           # conexión rechazada
./scripts/chaos.sh flaky 0.5      # 50% de errores 500 (aplicación)
./scripts/chaos.sh slow-discount  # sólo el endpoint de descuentos es lento
./scripts/chaos.sh status         # fallas activas + estado de los circuitos
./scripts/chaos.sh reset
```

Resultados observados (con `make load` corriendo y Grafana abierto):

| Falla | Qué pasa |
|---|---|
| `latency 1500` | El 1er pedido agota el presupuesto: **503 `quote-timeout`** en 2.5s. Los circuitos `stock`/`price` se abren por *slow calls* y los siguientes pedidos responden en ~0.4s con el precio cacheado (`pricingDegraded: true`) |
| `down` | Circuitos `OPEN`. SKUs con precio en cache: 202 degradado. SKUs nunca cotizados: **503 `catalog-unavailable`** + `Retry-After` |
| `slow-discount` | Sólo se pierde el descuento (`discountPercentage: 0`); el resto del pedido no se ve afectado |
| `reset` | A los 10s los circuitos pasan a `HALF_OPEN`; con 3 llamadas exitosas se cierran |
| Ráfaga con `demo-free-key` | 5 × 200 y el resto 429 (token bucket en Redis) |
| 2 pedidos concurrentes de `SKU-005` (stock 1) | Ambos pasan la validación previa; Kafka serializa la reserva: uno `CONFIRMED`, otro `REJECTED` |

## Observabilidad

- **Trazas**: un pedido genera **una sola traza** que cruza HTTP y Kafka (el outbox guarda el `traceparent`):
  ```
  api-gateway      HTTP POST
  order-service    http post /api/v1/orders
  catalog-service  http get …/stock        ┐ en paralelo
  catalog-service  http get …/price        ┘
  catalog-service  http get /api/v1/discounts      (después de price: thenCompose)
  order-service    outbox publish OrderCreated → orders.order-created.v1 send
  catalog-service  orders.order-created.v1 receive → outbox publish StockReserved → send
  order-service    catalog.stock-reserved.v1 receive
  ```
- **Logs**: `[servicio,traceId,correlationId]` en cada línea, incluso en threads del bulkhead y consumidores Kafka.
- **Métricas** (Grafana): estado de los circuitos, failure/slow rate, reintentos, bulkhead, degradaciones de la cotización por causa, 429 del gateway y del catálogo, latencia p50/p95/p99, outbox pendiente, resultados del saga y mensajes en el DLT.

## Tests

`make test` (32 tests; los de integración requieren Docker):

| Test | Qué verifica |
|---|---|
| `CatalogClientResilienceTest` (WireMock) | retry de 503, no-retry de 404, apertura del circuito a mitad de los reintentos, aislamiento entre circuitos, timeouts, 429 |
| `OrderQuoteServiceTest` | paralelismo real, fallbacks por dato, `completeOnTimeout`, `orTimeout`, fail-fast |
| `CatalogPactConsumerTest` → `contracts/pacts/*.json` | contrato que order-service espera del catálogo |
| `CatalogPactProviderTest` | el catálogo cumple ese contrato (rompe *su* build si lo viola) |
| `OrderFlowIT` (Testcontainers: Postgres + Kafka) | saga completo, Idempotency-Key (replay, mal uso), 503 con `Retry-After` |
| `StockReservationIT` (Testcontainers) | evento duplicado ⇒ stock descontado una sola vez, rechazo todo-o-nada, poison pill ⇒ DLT |

## Detalles sutiles (aparecieron construyendo esto)

1. **El contexto se pierde al completar el future.** Restaurar traceId/MDC *dentro* de la llamada HTTP no alcanza: el `CompletableFuture` se completa después, en un thread del bulkhead, y todo lo encadenado (`thenCompose` → llamada a discount, persistir, outbox) quedaba sin traza ni correlationId. `ResilientExecutor` completa el future dentro del contexto capturado.
2. **Un `ScheduledExecutorService` publicado como bean** es adoptado por Spring como scheduler de `@Scheduled`: el relay del outbox terminaba corriendo en los threads de Resilience4j. Por eso ese scheduler es privado.
3. **Los clientes HTTP pueden reintentar solos.** Apache HttpClient 5 (el que usa `TestRestTemplate`) respeta `Retry-After` y reintenta 503 automáticamente: sumado a nuestros reintentos multiplica la carga. Hay que saber qué hace cada capa.
4. **El circuito se abre a mitad de los reintentos**, y el reintento siguiente recibe `CallNotPermittedException` y corta. Ese es el motivo para poner Retry *afuera* del CircuitBreaker.
5. **La validación síncrona de stock es orientativa**: dos pedidos concurrentes la pasan; la reserva atómica del saga es la que decide.

## Estructura

```
├── pom.xml                  # padre multi-módulo (Boot 3.5, Cloud 2025.0, Resilience4j 2.3, Pact 4.6)
├── Dockerfile               # único, parametrizado por SERVICE, con capas de Spring Boot
├── docker-compose.yml
├── Makefile
├── api-gateway/
├── order-service/           # consumer: client/ quote/ domain/ messaging/ outbox/ idempotency/
├── catalog-service/         # provider: api/ domain/ messaging/ outbox/
├── contracts/pacts/         # contratos Pact generados por el consumer
├── infra/                   # prometheus, grafana (datasources + dashboard), toxiproxy
└── scripts/                 # demo.sh, chaos.sh
```

> Simplificaciones de demo: API keys en el YAML, cache de precios en memoria (con varias réplicas iría a Redis),
> un único broker Kafka, `/admin/faults` expuesto en el catálogo. Están marcadas en el código.
