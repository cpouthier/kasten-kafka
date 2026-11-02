#!/usr/bin/env bash
# Deploy a 3-node Kafka KRaft cluster (local Helm chart, official apache/kafka image)
# and seed it with deterministic fake data so a Veeam Kasten backup/restore can be
# checked with verify-data.sh.
set -euo pipefail

VERSION="1.0.0"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=fake-data.sh
source "$SCRIPT_DIR/fake-data.sh"

echo "deploy-kafka.sh v${VERSION}"

read -rp "Namespace [kafka]: " NAMESPACE
NAMESPACE=${NAMESPACE:-kafka}
read -rp "Helm release name [kafka]: " RELEASE
RELEASE=${RELEASE:-kafka}

echo
kubectl get storageclass
DEFAULT_SC=$(kubectl get storageclass -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}')
read -rp "CSI StorageClass with snapshot support [${DEFAULT_SC}]: " STORAGE_CLASS
STORAGE_CLASS=${STORAGE_CLASS:-$DEFAULT_SC}
[ -n "$STORAGE_CLASS" ] || { echo "No StorageClass given and no default StorageClass found"; exit 1; }

POD="${RELEASE}-0"
KBIN=/opt/kafka/bin

# Run a Kafka CLI tool inside the first node, with a small heap so it fits next to the broker
kexec() {
  kubectl exec -i -n "$NAMESPACE" "$POD" -c kafka -- env KAFKA_HEAP_OPTS="-Xmx256m" "$@"
}

echo
echo "Installing Kafka in namespace ${NAMESPACE}..."
helm upgrade --install "$RELEASE" "$SCRIPT_DIR/chart/kafka-kraft" -n "$NAMESPACE" --create-namespace \
  --set persistence.storageClass="$STORAGE_CLASS"

kubectl rollout status "statefulset/${RELEASE}" -n "$NAMESPACE" --timeout=600s
REPLICAS=$(kubectl get statefulset "$RELEASE" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}')
RF=$(( REPLICAS < 3 ? REPLICAS : 3 ))

echo "Waiting for ${REPLICAS} brokers to register..."
i=0
until [ "$(kexec $KBIN/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null | grep -c '(id:')" = "$REPLICAS" ]; do
  i=$((i+1)); [ "$i" -ge 60 ] && { echo "Brokers not registered"; exit 1; }
  sleep 5
done

echo
echo "Resetting test topics and consumer group (makes the seeding repeatable)..."
kexec $KBIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group "$CONSUMER_GROUP" >/dev/null 2>&1 || true
kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic 'shop-.*' 2>/dev/null
i=0
until [ "$(kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -c '^shop-' || true)" = "0" ]; do
  i=$((i+1)); [ "$i" -ge 30 ] && { echo "Old topics not deleted"; exit 1; }
  sleep 2
done

echo "Creating topics..."
kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC_CUSTOMERS" \
  --partitions 1 --replication-factor "$RF" --config cleanup.policy=compact
kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC_ORDERS" \
  --partitions 3 --replication-factor "$RF"
kexec $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC_EVENTS" \
  --partitions 1 --replication-factor "$RF"

produce() { # $1=topic, messages "key|value" on stdin
  kexec $KBIN/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$1" \
    --command-property acks=all --reader-property parse.key=true --reader-property key.separator='|'
}

echo "Producing fake data..."
gen_customers | produce "$TOPIC_CUSTOMERS"
gen_orders | produce "$TOPIC_ORDERS"
gen_events | produce "$TOPIC_EVENTS"

# Simulate a consumer that has processed the first messages of every orders partition
kexec $KBIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group "$CONSUMER_GROUP" \
  --topic "$TOPIC_ORDERS" --reset-offsets --to-offset "$GROUP_OFFSET" --execute >/dev/null

echo
kexec $KBIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic 'shop-.*'
echo
echo "Seeded ${COUNT_CUSTOMERS} customers, ${COUNT_ORDERS} orders, ${COUNT_EVENTS} events, consumer group ${CONSUMER_GROUP}"
echo
echo "Next: back up namespace ${NAMESPACE} with Veeam Kasten, then wipe the topics with"
echo "  kubectl exec -n ${NAMESPACE} ${POD} -c kafka -- $KBIN/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic 'shop-.*'"
echo "restore, and check the data with ./verify-data.sh"
