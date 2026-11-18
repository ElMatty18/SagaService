#!/usr/bin/env bash
# =====================================================================
# Escenarios de demo contra el stack levantado con `make up`.
#   ./scripts/demo.sh            corre todos
#   ./scripts/demo.sh <n>        corre uno: happy | idempotency | auth | ratelimit | saga | degraded | breaker
# =====================================================================
set -uo pipefail

GW=${GW:-http://localhost:8080}
ORDERS=${ORDERS:-http://localhost:8081}
FREE=demo-free-key
PREMIUM=demo-premium-key
DIR="$(cd "$(dirname "$0")" && pwd)"

title() { printf '\n\033[1;36m━━ %s ━━\033[0m\n' "$1"; }
note()  { printf '\033[0;90m%s\033[0m\n' "$1"; }

# post_order <api-key> <idempotency-key> <json-body>  => imprime "<status> <body>"
post_order() {
  local tmp code
  tmp=$(mktemp)
  code=$(curl -s -o "$tmp" -w '%{http_code}' -X POST "$GW/api/v1/orders" \
    -H "X-API-Key: $1" -H "Idempotency-Key: $2" -H 'Content-Type: application/json' -d "$3")
  echo "$code $(cat "$tmp")"
  rm -f "$tmp"
}

wait_final() {
  for _ in $(seq 1 20); do
    status=$(curl -s "$GW/api/v1/orders/$1" -H "X-API-Key: $PREMIUM" | jq -r .status)
    [[ "$status" != "PENDING" ]] && { echo "$status"; return; }
    sleep 0.5
  done
  echo "PENDING"
}

happy() {
  title "1. Camino feliz: pedido → saga → CONFIRMED"
  out=$(post_order $PREMIUM "demo-$(date +%s%N)" '{"customerId":"ana","items":[{"sku":"SKU-001","quantity":3},{"sku":"SKU-004","quantity":2}]}')
  echo "$out" | cut -d' ' -f2- | jq '{id, status, total, pricingDegraded, lines: [.lines[] | {sku, quantity, unitPrice, discountPercentage, lineTotal}]}'
  id=$(echo "$out" | cut -d' ' -f2- | jq -r .id)
  note "202 Accepted: el pedido queda PENDING hasta que catalog-service reserve el stock (Kafka)."
  echo "Estado final: $(wait_final "$id")"
}

idempotency() {
  title "2. Idempotency-Key: el mismo POST dos veces"
  key="idem-$(date +%s%N)"
  body='{"customerId":"bob","items":[{"sku":"SKU-002","quantity":1}]}'
  a=$(post_order $PREMIUM "$key" "$body"); b=$(post_order $PREMIUM "$key" "$body")
  echo "1º: $(echo "$a" | cut -d' ' -f1)  id=$(echo "$a" | cut -d' ' -f2- | jq -r .id)"
  echo "2º: $(echo "$b" | cut -d' ' -f1)  id=$(echo "$b" | cut -d' ' -f2- | jq -r .id)   ← mismo pedido, sin duplicar"
  c=$(post_order $PREMIUM "$key" '{"customerId":"bob","items":[{"sku":"SKU-002","quantity":9}]}')
  echo "misma key, otro body: $(echo "$c" | cut -d' ' -f1) $(echo "$c" | cut -d' ' -f2- | jq -r .type)"
}

auth() {
  title "3. Autenticación en el gateway"
  echo "sin API key:     $(curl -s -o /dev/null -w '%{http_code}' "$GW/api/v1/products")"
  echo "API key inválida: $(curl -s -o /dev/null -w '%{http_code}' -H 'X-API-Key: nope' "$GW/api/v1/products")"
  echo "API key válida:   $(curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: $FREE" "$GW/api/v1/products")"
}

ratelimit() {
  title "4. Rate limit por plan (token bucket en Redis)"
  for plan in FREE PREMIUM; do
    key=$([[ $plan == FREE ]] && echo $FREE || echo $PREMIUM)
    codes=$(for _ in $(seq 1 15); do curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $key" "$GW/api/v1/products" & done; wait)
    echo "$plan: 15 requests simultáneos => $(echo "$codes" | grep -c 200) x 200, $(echo "$codes" | grep -c 429) x 429"
  done
  curl -s -D - -o /dev/null -H "X-API-Key: $PREMIUM" "$GW/api/v1/products" | grep -i '^x-ratelimit'
  note "FREE: burst 5 / 2 req/s. PREMIUM: burst 40 / 20 req/s."
  sleep 3
}

saga() {
  title "5. Saga: la validación síncrona de stock es ORIENTATIVA, la reserva es la verdad"
  note "SKU-005 tiene stock 1. Dos pedidos concurrentes pasan la validación previa; la reserva (Kafka) decide."
  local a b
  a=$(mktemp); b=$(mktemp)
  post_order $PREMIUM "saga-a-$(date +%s%N)" '{"customerId":"c1","items":[{"sku":"SKU-005","quantity":1}]}' > "$a" &
  post_order $PREMIUM "saga-b-$(date +%s%N)" '{"customerId":"c2","items":[{"sku":"SKU-005","quantity":1}]}' > "$b" &
  wait
  for f in "$a" "$b"; do
    r=$(cat "$f"); code=${r%% *}; id=$(echo "${r#* }" | jq -r '.id // empty')
    echo "pedido => HTTP $code${id:+, estado final: $(wait_final "$id")}"
  done
  rm -f "$a" "$b"
  note "Esperado: uno CONFIRMED y otro REJECTED (o 409 si ya no había stock al cotizar)."
}

degraded() {
  title "6. Degradación parcial: descuento lento => pedido sin descuento (no falla)"
  "$DIR/chaos.sh" slow-discount 1500 >/dev/null
  post_order $PREMIUM "deg-$(date +%s%N)" '{"customerId":"dana","items":[{"sku":"SKU-001","quantity":3}]}' \
    | cut -d' ' -f2- | jq '{status, total, pricingDegraded, lines: [.lines[] | {sku, discountPercentage}]}'
  "$DIR/chaos.sh" reset >/dev/null
  note "completeOnTimeout(400ms) en OrderQuoteService: el descuento es opcional."
}

breaker() {
  title "7. Circuit breaker: catálogo caído"
  # Asegura un precio en cache para SKU-001 (fallback "last known price")
  post_order $PREMIUM "warm-$(date +%s%N)" '{"customerId":"eve","items":[{"sku":"SKU-001","quantity":1}]}' >/dev/null
  "$DIR/chaos.sh" down
  for i in $(seq 1 6); do
    r=$(post_order $PREMIUM "cb-$i-$(date +%s%N)" '{"customerId":"eve","items":[{"sku":"SKU-001","quantity":1}]}')
    echo "SKU-001 #$i => $(echo "$r" | cut -d' ' -f1) pricingDegraded=$(echo "$r" | cut -d' ' -f2- | jq -r '.pricingDegraded // .type')"
  done
  r=$(post_order $PREMIUM "cb-x-$(date +%s%N)" '{"customerId":"eve","items":[{"sku":"SKU-003","quantity":1}]}')
  echo "SKU-003 (sin precio en cache) => $(echo "$r" | cut -d' ' -f1) $(echo "$r" | cut -d' ' -f2- | jq -r .type)"
  "$DIR/chaos.sh" status | tail -4
  "$DIR/chaos.sh" up
  note "Tras 10s el circuito pasa a HALF_OPEN; 3 llamadas OK lo cierran. Mirá Grafana: http://localhost:3000"
}

case "${1:-all}" in
  all) happy; idempotency; auth; ratelimit; saga; degraded; breaker ;;
  *) "$1" ;;
esac
