#!/usr/bin/env bash
# Loads examples/data/orders.csv into the 'orders' index of the local Elasticsearch
# (docker compose -f deploy/docker-compose.yml up -d elasticsearch), recreating it.
# The row without an order_id, the negative amount and the 3-letter country are there on
# purpose: examples/elasticsearch_to_mongodb.yaml quarantines them.
#   ES_URL=http://user:pass@host:9200 deploy/seed_elasticsearch.sh   # another cluster
set -euo pipefail
cd "$(dirname "$0")/.."
ES_URL="${ES_URL:-http://localhost:9200}"

for _ in $(seq 1 30); do
  curl -fs "$ES_URL/_cluster/health" >/dev/null 2>&1 && break
  sleep 2
done

curl -fs -XDELETE "$ES_URL/orders" >/dev/null 2>&1 || true
curl -fs -XPUT "$ES_URL/orders" -H 'Content-Type: application/json' -d '{
  "mappings": {"properties": {
    "order_id": {"type": "integer"}, "customer_id": {"type": "integer"}, "amount": {"type": "double"},
    "country": {"type": "keyword"}, "order_ts": {"type": "date"}
  }}
}' >/dev/null

bulk=$(tail -n +2 examples/data/orders.csv | while IFS=, read -r order_id customer_id amount country order_ts; do
  if [ -n "$order_id" ]; then printf '{"index":{"_index":"orders","_id":"%s"}}\n' "$order_id"; else printf '{"index":{"_index":"orders"}}\n'; fi
  printf '{%s"customer_id":%s,"amount":%s,"country":"%s","order_ts":"%sZ"}\n' \
    "$([ -n "$order_id" ] && printf '"order_id":%s,' "$order_id")" "$customer_id" "$amount" "$country" "${order_ts/ /T}"
done)
printf '%s\n' "$bulk" | curl -fs -XPOST "$ES_URL/_bulk?refresh=true" -H 'Content-Type: application/x-ndjson' --data-binary @- \
  | python3 -c '
import json, sys
items = [i["index"] for i in json.load(sys.stdin)["items"]]
bad = [i for i in items if "error" in i]
print(f"indexed {len(items) - len(bad)} documents into orders" + (f"; {len(bad)} failed: {bad[0]}" if bad else ""))
'
