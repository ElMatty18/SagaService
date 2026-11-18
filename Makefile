.DEFAULT_GOAL := help
COMPOSE := docker compose

help: ## Lista los comandos
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

build: ## Construye las imágenes de los 3 servicios
	$(COMPOSE) build

up: ## Levanta todo el stack (y espera a que esté healthy)
	$(COMPOSE) up -d --build --wait
	@echo ""
	@echo "  Gateway     http://localhost:8080   (X-API-Key: demo-free-key | demo-premium-key)"
	@echo "  Grafana     http://localhost:3000"
	@echo "  Jaeger      http://localhost:16686"
	@echo "  Kafka UI    http://localhost:8090"
	@echo "  Prometheus  http://localhost:9090"

down: ## Detiene el stack
	$(COMPOSE) down

clean: ## Detiene el stack y borra volúmenes (datos)
	$(COMPOSE) down -v --remove-orphans

ps: ## Estado de los contenedores
	$(COMPOSE) ps

logs: ## Logs de los servicios Java
	$(COMPOSE) logs -f api-gateway order-service catalog-service

test: ## Tests: unitarios, contratos Pact e integración (Testcontainers; requiere Docker)
	mvn -B verify

demo: ## Corre todos los escenarios de demo (ver scripts/demo.sh)
	./scripts/demo.sh

chaos-latency: ## Inyecta 1500ms de latencia order → catalog
	./scripts/chaos.sh latency 1500

chaos-down: ## Corta la conexión order → catalog
	./scripts/chaos.sh down

chaos-flaky: ## 50% de errores 500 en el catálogo
	./scripts/chaos.sh flaky 0.5

chaos-reset: ## Quita todas las fallas
	./scripts/chaos.sh reset

chaos-status: ## Fallas activas y estado de los circuit breakers
	./scripts/chaos.sh status

load: ## Genera tráfico continuo (Ctrl+C para cortar) para ver el dashboard en acción
	@while true; do \
	  curl -s -o /dev/null -w '%{http_code} ' -X POST http://localhost:8080/api/v1/orders \
	    -H 'X-API-Key: demo-premium-key' -H "Idempotency-Key: load-$$(date +%s%N)" -H 'Content-Type: application/json' \
	    -d '{"customerId":"load","items":[{"sku":"SKU-002","quantity":3}]}'; \
	  sleep 0.2; \
	done

.PHONY: help build up down clean ps logs test demo chaos-latency chaos-down chaos-flaky chaos-reset chaos-status load
