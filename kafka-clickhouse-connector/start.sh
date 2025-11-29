#!/bin/bash
/kafka/bin/connect-distributed.sh /kafka/config/connect-distributed.properties &
sleep 20  # Warte, bis Connect bereit ist
curl -X POST -H "Content-Type: application/json" --data @/kafka/connectors/config.json http://localhost:8083/connectors
wait
