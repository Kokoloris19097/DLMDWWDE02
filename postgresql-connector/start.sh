#!/bin/bash
set -e

echo "Starting Kafka Connect..."

# Warte auf Kafka
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b $CONNECT_BOOTSTRAP_SERVERS 1 60

# Starte Kafka Connect im Hintergrund
/etc/confluent/docker/run &
CONNECT_PID=$!

# Warte bis Connect REST API verfügbar ist
echo "Waiting for Kafka Connect REST API..."
while ! curl -s http://localhost:8083/ > /dev/null 2>&1; do
    echo "  Waiting for REST API..."
    sleep 5
done
echo "Kafka Connect REST API is ready!"

# Warte bis Kafka Connect vollständig initialisiert ist (keine 500er mehr)
echo "Waiting for Kafka Connect to be fully initialized..."
MAX_WAIT=120
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors)
    if [ "$HTTP_CODE" = "200" ]; then
        echo "Kafka Connect is fully initialized!"
        break
    fi
    echo "  Still initializing... (HTTP $HTTP_CODE, ${WAITED}s/${MAX_WAIT}s)"
    sleep 5
    WAITED=$((WAITED + 5))
done

# Warte auf PostgreSQL
echo "Waiting for PostgreSQL..."
while ! nc -z postgresql.data.svc.cluster.local 5432 2>/dev/null; do
    echo "  Waiting for PostgreSQL..."
    sleep 5
done
echo "PostgreSQL is ready!"

# Zusätzliche Wartezeit für Stabilität
sleep 5

# Registriere Connector
echo "Registering PostgreSQL Sink Connector..."
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
-d @/etc/kafka-connect/connector-config.json)

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
    echo "Connector registered successfully!"
    elif [ "$HTTP_CODE" = "409" ]; then
    echo "Connector already exists."
else
    echo "Response: HTTP $HTTP_CODE"
    echo "$BODY"
fi

# Zeige Connector Status
sleep 3
echo ""
echo "=== Connector Status ==="
curl -s http://localhost:8083/connectors
echo ""
curl -s http://localhost:8083/connectors/postgresql-sink/status 2>/dev/null || echo "Status not available yet"
echo ""
echo "========================"

echo "Kafka Connect is running."

# Halte Container am Leben
wait $CONNECT_PID
