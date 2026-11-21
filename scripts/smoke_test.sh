#!/usr/bin/env bash
# End-to-end check against a running stack (make up): a post fans out through Kafka
# into a follower's Redis feed.
set -euo pipefail
API=${API:-http://localhost:8080}

for _ in $(seq 60); do curl -sf "$API/health" >/dev/null && break; sleep 2; done
curl -sf "$API/health" >/dev/null

suffix=$RANDOM
author=$(curl -sf -X POST "$API/users" -d "{\"username\":\"author$suffix\"}" | jq -r .id)
reader=$(curl -sf -X POST "$API/users" -d "{\"username\":\"reader$suffix\"}" | jq -r .id)
curl -sf -X POST "$API/users/$reader/follow" -d "{\"target_id\":$author}" >/dev/null
curl -sf -X POST "$API/posts" -d "{\"user_id\":$author,\"content\":\"hello $suffix\"}" >/dev/null

for _ in $(seq 30); do
  total=$(curl -sf "$API/feed/$reader" | jq -r .total)
  if [ "$total" -ge 1 ]; then
    echo "ok: post reached the follower's feed (total=$total)"
    exit 0
  fi
  sleep 1
done
echo "FAIL: post never reached the follower's feed" >&2
exit 1
