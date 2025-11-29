#!/bin/bash
set -e

echo "Starting Kafka Connect..."

# Warte auf Kafka
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b $CONNECT_BOOTSTRAP_SERVERS 1 60

# Starte Kafka Connect im Hintergrund
/etc/confluent/docker/run &

# Warte bis Connect REST API verfügbar ist
echo "Waiting for Kafka Connect REST API..."
while ! curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    sleep 2
done

echo "Kafka Connect is ready!"

# Warte auf PostgreSQL
echo "Waiting for PostgreSQL..."
while ! nc -z postgresql.data.svc.cluster.local 5432; do
    sleep 2
done

echo "PostgreSQL is ready!"

# Registriere Connector (falls nicht existiert)
if ! curl -s http://localhost:8083/connectors/postgresql-sink > /dev/null 2>&1; then
    echo "Registering PostgreSQL Sink Connector..."
    curl -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d @/etc/kafka-connect/connector-config.json
    echo "Connector registered!"
else
    echo "Connector already exists, updating..."
    curl -X PUT http://localhost:8083/connectors/postgresql-sink/config \
        -H "Content-Type: application/json" \
        -d "$(cat /etc/kafka-connect/connector-config.json | jq '.config')"
fi

# Halte Container am Leben
wait
