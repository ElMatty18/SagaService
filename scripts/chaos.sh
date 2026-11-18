#!/usr/bin/env bash
# =====================================================================
# Inyección de fallas entre order-service y catalog-service.
#
#  Red (Toxiproxy, afecta a TODAS las llamadas order → catalog):
#    latency [ms]     agrega latencia (default 1500ms) => timeouts, slow calls
#    timeout          la conexión queda colgada sin responder => read timeout
#    down             corta la conexión (connection refused) => fallas inmediatas
#    up               restablece la conexión
#  Aplicación (catalog-service /admin/faults, afecta a un endpoint puntual):
#    flaky [rate]     % de errores 500 en stock/price (default 0.5)
#    slow-discount [ms]  sólo el endpoint de descuentos es lento (default 1000ms)
#  General:
#    reset            quita todas las fallas
#    status           toxics activos + estado de los circuit breakers
# =====================================================================
set -euo pipefail

TOXI=${TOXI:-http://localhost:8474}
CATALOG=${CATALOG:-http://localhost:8082}
ORDERS=${ORDERS:-http://localhost:8081}
PROXY=catalog

toxic() { curl -fsS -X POST "$TOXI/proxies/$PROXY/toxics" -H 'Content-Type: application/json' -d "$1" >/dev/null; }
remove_toxics() {
  for t in $(curl -fsS "$TOXI/proxies/$PROXY/toxics" | jq -r '.[].name'); do
    curl -fsS -X DELETE "$TOXI/proxies/$PROXY/toxics/$t" >/dev/null
  done
}
faults() { curl -fsS -X PUT "$CATALOG/admin/faults" -H 'Content-Type: application/json' -d "$1"; echo; }

case "${1:-status}" in
  latency)
    remove_toxics
    toxic "{\"name\":\"latency\",\"type\":\"latency\",\"stream\":\"downstream\",\"attributes\":{\"latency\":${2:-1500},\"jitter\":100}}"
    echo "⏳ Latencia de ${2:-1500}ms entre order-service y catalog-service" ;;
  timeout)
    remove_toxics
    toxic '{"name":"timeout","type":"timeout","stream":"downstream","attributes":{"timeout":0}}'
    echo "🕳️  Las respuestas del catálogo nunca llegan (timeout)" ;;
  down)
    curl -fsS -X POST "$TOXI/proxies/$PROXY" -H 'Content-Type: application/json' -d '{"enabled":false}' >/dev/null
    echo "💥 catalog-service inalcanzable desde order-service" ;;
  up)
    curl -fsS -X POST "$TOXI/proxies/$PROXY" -H 'Content-Type: application/json' -d '{"enabled":true}' >/dev/null
    echo "✅ Conexión restablecida" ;;
  flaky)
    faults "{\"discountLatencyMs\":0,\"productErrorRate\":${2:-0.5}}" ;;
  slow-discount)
    faults "{\"discountLatencyMs\":${2:-1000},\"productErrorRate\":0}" ;;
  reset)
    curl -fsS -X POST "$TOXI/reset" >/dev/null
    faults '{"discountLatencyMs":0,"productErrorRate":0}' >/dev/null
    echo "🧹 Sin fallas inyectadas" ;;
  status)
    echo "── Toxiproxy ──────────────────────────────"
    curl -fsS "$TOXI/proxies/$PROXY" | jq '{enabled, toxics: [.toxics[] | {name, type, attributes}]}'
    echo "── Fallas de aplicación (catalog) ─────────"
    curl -fsS "$CATALOG/admin/faults" | jq -c .
    echo "── Circuit breakers (order-service) ───────"
    curl -fsS "$ORDERS/actuator/circuitbreakers" \
      | jq -r '.circuitBreakers | to_entries[] | "\(.key)\t\(.value.state)\tfailureRate=\(.value.failureRate)\tslowRate=\(.value.slowCallRate)\tnotPermitted=\(.value.notPermittedCalls)"' \
      | column -t ;;
  *)
    sed -n '3,17p' "$0"; exit 1 ;;
esac
