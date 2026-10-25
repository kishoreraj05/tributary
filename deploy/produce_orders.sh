#!/usr/bin/env bash
# Sends a few JSON orders (one bad) to the 'orders' topic of the local Redpanda.
set -euo pipefail
cd "$(dirname "$0")"
docker compose exec -T redpanda rpk topic create orders >/dev/null 2>&1 || true
printf '%s\n' \
  '{"order_id": 1, "customer_id": 100, "amount": 25.5, "country": "in"}' \
  '{"order_id": 2, "customer_id": 101, "amount": -3.0, "country": "us"}' \
  '{"order_id": 3, "customer_id": 102, "amount": 12.0, "country": "de"}' \
  | docker compose exec -T redpanda rpk topic produce orders
