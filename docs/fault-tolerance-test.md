# Kafka Fault-Tolerance Test

## Objective

Verify that the Online Education Activity Monitoring pipeline
continues to operate when one Kafka broker becomes unavailable.

## Cluster

- Broker 1: localhost:9092
- Broker 2: localhost:9094
- Broker 3: localhost:9095

## Topic

education-events

Expected:

- Partitions: 3
- Replication Factor: 3
- Minimum ISR: 2

## Test 1 — Verify Cluster

Check all Kafka containers:

```bash
docker ps