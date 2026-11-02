# Deterministic fake data for the Kafka test workload, sourced by deploy-kafka.sh and verify-data.sh.
# Each generator prints one "key|value" line per message.

TOPIC_CUSTOMERS="shop-customers"   # 1 partition, compacted
TOPIC_ORDERS="shop-orders"         # 3 partitions
TOPIC_EVENTS="shop-events"         # 1 partition
CONSUMER_GROUP="billing-service"   # committed at offset GROUP_OFFSET on every shop-orders partition
GROUP_OFFSET=10

COUNT_CUSTOMERS=10
COUNT_ORDERS=100
COUNT_EVENTS=50

CUSTOMER_NAMES=("Alice Martin" "Bruno Leroy" "Chloe Dubois" "David Moreau" "Emma Laurent"
                "Felix Garcia" "Gabrielle Roux" "Hugo Fournier" "Ines Girard" "Jules Bonnet")
CUSTOMER_CITIES=("Paris" "Lyon" "Marseille" "Toulouse" "Nantes" "Lille" "Bordeaux" "Nice" "Rennes" "Grenoble")
PRODUCTS=("laptop" "monitor" "keyboard" "mouse" "headset" "webcam" "dock" "ssd")
PRICES_CENTS=(129900 34900 8900 4900 15900 7900 19900 11900)
EVENT_TYPES=("login" "page_view" "add_to_cart" "checkout" "logout")

gen_customers() {
  local i id name email
  for i in $(seq 0 $((COUNT_CUSTOMERS - 1))); do
    id=$(printf 'cust-%02d' $((i + 1)))
    name=${CUSTOMER_NAMES[$i]}
    email=$(echo "$name" | tr '[:upper:] ' '[:lower:].')@example.com
    printf '%s|{"customer_id":"%s","name":"%s","city":"%s","email":"%s"}\n' \
      "$id" "$id" "$name" "${CUSTOMER_CITIES[$i]}" "$email"
  done
}

gen_orders() {
  local n p qty total minutes
  for n in $(seq 1 "$COUNT_ORDERS"); do
    p=$(( (n * 3) % ${#PRODUCTS[@]} ))
    qty=$(( n % 5 + 1 ))
    total=$(( qty * PRICES_CENTS[p] ))
    minutes=$(( n * 17 ))
    printf 'ord-%03d|{"order_id":"ord-%03d","customer_id":"cust-%02d","product":"%s","quantity":%d,"total":%d.%02d,"created_at":"2026-01-%02dT%02d:%02d:00Z"}\n' \
      "$n" "$n" $(( (n * 7) % COUNT_CUSTOMERS + 1 )) "${PRODUCTS[$p]}" "$qty" \
      $(( total / 100 )) $(( total % 100 )) \
      $(( 1 + minutes / 1440 )) $(( (minutes / 60) % 24 )) $(( minutes % 60 ))
  done
}

gen_events() {
  local n
  for n in $(seq 1 "$COUNT_EVENTS"); do
    printf 'evt-%03d|{"event_id":"evt-%03d","customer_id":"cust-%02d","type":"%s","ts":"2026-01-01T%02d:%02d:00Z"}\n' \
      "$n" "$n" $(( n % COUNT_CUSTOMERS + 1 )) "${EVENT_TYPES[$(( n % ${#EVENT_TYPES[@]} ))]}" \
      $(( 8 + n / 60 )) $(( n % 60 ))
  done
}
