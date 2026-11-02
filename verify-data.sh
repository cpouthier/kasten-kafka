#!/usr/bin/env bash
# Check that the fake data seeded by deploy-kafka.sh is intact (run after a Veeam Kasten restore).
set -euo pipefail

VERSION="1.0.0"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=fake-data.sh
source "$SCRIPT_DIR/fake-data.sh"

echo "verify-data.sh v${VERSION}"

read -rp "Namespace [kafka]: " NAMESPACE
NAMESPACE=${NAMESPACE:-kafka}
read -rp "Helm release name [kafka]: " RELEASE
RELEASE=${RELEASE:-kafka}

POD="${RELEASE}-0"
KBIN=/opt/kafka/bin
VERIFY_GROUP="kasten-verify"

kexec() {
  kubectl exec -i -n "$NAMESPACE" "$POD" -c kafka -- env KAFKA_HEAP_OPTS="-Xmx256m" "$@"
}

FAILED=0
check() { # $1=label $2=expected $3=actual
  if [ "$2" = "$3" ]; then
    echo "  OK    $1"
  else
    echo "  FAIL  $1 (expected: $2, got: ${3:-nothing})"
    FAILED=1
  fi
}

# Sum of the end offsets of a topic = number of messages (nothing is ever deleted from these topics)
message_count() {
  kexec $KBIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null \
    | awk -F: '{ s += $3 } END { print s + 0 }'
}

# All messages of a topic as sorted "key|value" lines
topic_content() { # $1=topic $2=expected count
  kexec $KBIN/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$1" \
    --group "$VERIFY_GROUP" --from-beginning --max-messages "$2" --timeout-ms 20000 \
    --formatter-property print.key=true --formatter-property key.separator='|' \
    --command-property enable.auto.commit=false 2>/dev/null | sort
}

check_topic() { # $1=topic $2=partitions $3=expected count $4=generator
  local desc
  desc=$(kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$1" 2>/dev/null | awk 'NR == 1' || true)
  check "$1 partitions"  "$2" "$(echo "$desc" | sed -n 's/.*PartitionCount: \([0-9]*\).*/\1/p')"
  check "$1 messages"    "$3" "$(message_count "$1")"
  if [ "$($4 | sort)" = "$(topic_content "$1" "$3")" ]; then
    echo "  OK    $1 content"
  else
    echo "  FAIL  $1 content differs from the seeded data"
    FAILED=1
  fi
}

echo
echo "Brokers registered: $(kexec $KBIN/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep -c '(id:' || true)"

check_topic "$TOPIC_CUSTOMERS" 1 "$COUNT_CUSTOMERS" gen_customers
check "$TOPIC_CUSTOMERS cleanup.policy" "compact" \
  "$(kexec $KBIN/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics --entity-name "$TOPIC_CUSTOMERS" 2>/dev/null \
     | sed -n 's/^ *cleanup\.policy=\([a-z]*\) .*/\1/p')"
check_topic "$TOPIC_ORDERS" 3 "$COUNT_ORDERS" gen_orders
check_topic "$TOPIC_EVENTS" 1 "$COUNT_EVENTS" gen_events

# Committed offsets of the consumer group: GROUP_OFFSET on each of the 3 partitions
# (key partitioning is deterministic and every partition holds more than GROUP_OFFSET orders)
check "consumer group $CONSUMER_GROUP committed offsets" "$((GROUP_OFFSET * 3))" \
  "$(kexec $KBIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$CONSUMER_GROUP" 2>/dev/null \
     | awk -v g="$CONSUMER_GROUP" '$1 == g && $4 ~ /^[0-9]+$/ { s += $4 } END { print s + 0 }')"

check "under-replicated partitions" "0" \
  "$(kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null | grep -c 'Partition:' || true)"

kexec $KBIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group "$VERIFY_GROUP" >/dev/null 2>&1 || true

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All test data restored correctly"
else
  echo "Test data check FAILED"
  exit 1
fi
