# DLMDWWDE02
Master Data Engineering - System, was in der Lage ist, kontinuierlich massive Datenmengen auf zunehmen, diese auf effiziente Weise zu speichern, zu prozessieren, zu aggregieren, und für die direkte Nutzung  in einer Echtzeit-Reporting Applikation zur Verfügung zu stellen

## Requirements
- Docker Engine
- DockerCLI (winget install Docker.DockerCLI)
- Kubectl (winget install -e --id Kubernetes.kubectl)
- Helm (winget install Helm.Helm)
- kubectl

## Technologie Stack

### Container-Orchestrierung & Infrastructure
- **Kubernetes** - Container-Orchestrierung und Cluster-Management
- **Docker** - Containerisierung der Anwendungen
- **Helm** - Infrastructure as Code für Kubernetes

### Message Streaming & Data Ingestion
- **Apache Kafka** - Message Broker für Stream Processing
- **Kafka Connect** - Konnektoren für Datenintegration
- **FastAPI** - API-Framework für Datenaufnahme

### Batch Processing & Analytics
- **Apache Spark** - Distributed Computing für Batch-Prozessierung
- **ClickHouse** - Column-oriented Database für Analytics
- **MinIO** - S3-kompatible Objektspeicherung für Datenarchivierung

### Caching & Performance
- **Redis** - In-Memory Database für Caching

### Monitoring & Observability
- **Prometheus** - Metriken-Sammlung und Monitoring
- **Grafana** - Visualisierung und Dashboards

### Programmiersprachen & Frameworks
- **Python** - Hauptprogrammiersprache
- **FastAPI** - REST API Framework

### Images
- **Python** - [python:3.13.9-alpine3.22](https://hub.docker.com/_/python)
    - **FastAPI** - mit [FastAPI](https://pypi.org/project/fastapi/)
- **Redis** - [redis:8.2.2-alpine3.22](https://hub.docker.com/_/redis)
- **Apache Kafka** - [apache/kafka:4.0.1](https://hub.docker.com/r/apache/kafka)
- **ClickHouse** - [clickhouse/clickhouse-server:latest](https://hub.docker.com/r/clickhouse/clickhouse-server)
- **ClickHouse Connector** - [ClickHouse Kafka Connect Sink](https://github.com/ClickHouse/clickhouse-kafka-connect)
- **MinIO** - [minio/minio:latest](https://hub.docker.com/r/minio/minio)
- **MinIO/S3 Connector** - [Confluent S3 Sink Connector](https://www.confluent.io/hub/confluentinc/kafka-connect-s3)
- **Apache Spark** - [apache/spark:3.5.7](https://hub.docker.com/r/apache/spark)
- **Prometheus** - [prom/prometheus:v3.7.1](https://hub.docker.com/r/prom/prometheus)
- **Grafana** - [grafana/grafana:main-ubuntu](https://hub.docker.com/r/grafana/grafana)

## Architektur
### Datenfluss
![Datenflussdiagramm](doku/Datenfluss.jpg)

### K8s-Architektur
![Kubernetes-Architektur](doku/K8s-Architektur.jpg)
