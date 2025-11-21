# Apache Spark Deployment Script
# DLMDWWDE02 Master Project

Write-Host "\n=== Starte Spark Deployment ===" -ForegroundColor Cyan

# Beispiel: Starte Spark Container lokal (Docker)
docker build -t spark-app -f Dockerfile.spark .
docker run --rm -it --name spark-app spark-app

Write-Host "\n=== Spark Deployment abgeschlossen ===" -ForegroundColor Green
