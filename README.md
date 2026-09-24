# 🎓 Online Education Activity Monitoring

A real-time **Data Engineering pipeline for monitoring online education activity** using **Apache Kafka, Apache Spark Structured Streaming, HDFS, Apache Hive, and Apache HBase**.

The system captures student activity events such as logins, video interactions, quiz attempts, and course completion in real time. Kafka provides the distributed event-streaming layer, Spark performs real-time processing, HDFS provides durable storage, Hive enables analytical queries, and HBase is used for storing student activity alerts.

---

## 📌 Project Overview

Modern online learning platforms generate a continuous stream of user activity:

* Student logins
* Video starts
* Video completions
* Quiz starts
* Quiz submissions
* Course completions

Processing these events manually or in periodic batches makes it difficult to monitor student activity in near real time.

This project implements a **streaming data engineering architecture** that continuously processes education events.

### Core Pipeline

```text
                ┌──────────────────────┐
                │   Scala Event        │
                │     Producer         │
                └──────────┬───────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │      Apache Kafka       │
              │                         │
              │ Broker 1   Broker 2     │
              │ Broker 3               │
              │                         │
              │ education-events        │
              │ 3 Partitions            │
              │ Replication Factor = 3  │
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │ Apache Spark Structured │
              │       Streaming         │
              └────────────┬────────────┘
                           │
              ┌────────────┼─────────────┐
              ▼            ▼             ▼
          ┌───────┐    ┌───────┐    ┌────────┐
          │ HDFS  │    │ Hive  │    │ HBase  │
          │ Raw   │    │ SQL   │    │Alerts  │
          │ Data  │    │Analytics│   │        │
          └───────┘    └───────┘    └────────┘
```

---

# 🎯 Objectives

The project is designed to demonstrate practical Data Engineering concepts including:

* Real-time event streaming
* Distributed messaging with Kafka
* Kafka partitions and replication
* Kafka fault tolerance
* Spark Structured Streaming
* Micro-batch processing
* Distributed storage using HDFS
* Analytical processing using Hive
* Stateful student activity monitoring
* Low-latency alert storage using HBase
* End-to-end data pipeline design

---

# 🛠️ Technology Stack

| Technology    |            Version | Purpose                         |
| ------------- | -----------------: | ------------------------------- |
| Scala         |            2.12.18 | Producer and Spark applications |
| Apache Kafka  |              4.3.1 | Event streaming                 |
| Apache Spark  |              3.5.9 | Real-time processing            |
| Apache Hadoop |              3.3.6 | Distributed storage             |
| Apache Hive   |              3.1.2 | SQL analytics                   |
| Apache HBase  |              2.5.9 | Alert storage                   |
| sbt           |             1.11.7 | Scala build management          |
| Docker        |     Docker Desktop | Kafka cluster deployment        |
| WSL Ubuntu    | Ubuntu environment | Hadoop/Spark execution          |

---

# 📂 Project Structure

```text
online-education-monitoring/
│
├── docker-compose.yml
├── README.md
│
├── producer/
│   ├── build.sbt
│   └── src/
│       └── main/
│           └── scala/
│               └── EducationEventProducer.scala
│
├── spark/
│   ├── build.sbt
│   └── src/
│       └── main/
│           └── scala/
│               ├── EducationActivityStreaming.scala
│               ├── CourseCompletionAnalytics.scala
│               ├── StudentInactivityDetection.scala
│               └── HBaseAlertWriter.scala
│
├── hive/
│   ├── 01_create_database.sql
│   ├── 02_create_raw_events.sql
│   └── 03_analytics.sql
│
├── hbase/
│   └── 01_create_alert_table.hbase
│
├── data/
│
└── docs/
    └── 13_fault_tolerance_test.md
```

---

# 🧩 Component Responsibilities

## 1. Scala Producer

The producer generates synthetic education activity events.

Each event contains:

```text
event_id
student_id
session_id
course_id
event_type
event_time
device
duration
```

Example:

```text
E000001,S103,SESSION-S103-2,C101,LOGIN,2026-09-24T06:10:00Z,WEB,120
```

The student's ID is used as the Kafka message key.

This provides consistent partitioning for events belonging to the same student.

---

# 2. Apache Kafka

Kafka acts as the **distributed event streaming platform**.

The project uses a three-broker Kafka cluster:

```text
Broker 1 → localhost:9092
Broker 2 → localhost:9094
Broker 3 → localhost:9095
```

Kafka topic:

```text
education-events
```

Topic configuration:

```text
Partitions            = 3
Replication Factor    = 3
Min In-Sync Replicas   = 2
```

### Why three partitions?

Partitions allow Kafka to distribute event processing and provide parallelism.

```text
education-events

Partition 0
Partition 1
Partition 2
```

### Why replication factor 3?

Every partition has three replicas distributed across the three brokers.

For example:

```text
Partition 0
Leader → Broker 3
Replica → Broker 1
Replica → Broker 2
```

This protects the topic against a single broker failure.

---

# 3. Spark Structured Streaming

Spark continuously reads events from Kafka.

The main streaming application:

```text
EducationActivityStreaming.scala
```

Spark reads:

```text
Kafka
   ↓
Kafka DataFrame
   ↓
Parse key/value
   ↓
Streaming DataFrame
   ↓
Console / Storage
```

Kafka records are converted into a Spark streaming DataFrame.

The application uses:

```scala
spark.readStream
```

with the Kafka source:

```scala
.format("kafka")
```

and subscribes to:

```text
education-events
```

Spark processes incoming records using micro-batches.

---

# 4. HDFS

HDFS is used as the durable storage layer for raw education events.

Target location:

```text
/online-education/raw-events
```

The architecture separates:

```text
Streaming Layer
      ↓
Processing Layer
      ↓
Storage Layer
```

This allows the raw events to be retained for later analysis.

---

# 5. Apache Hive

Hive provides SQL-based analytics over the stored education events.

Database:

```text
education_monitoring
```

Table:

```text
raw_events
```

Example analytical queries include:

### Total events

```sql
SELECT COUNT(*)
FROM raw_events;
```

### Events by student

```sql
SELECT
    student_id,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY student_id;
```

### Events by event type

```sql
SELECT
    split(event, ',')[4] AS event_type,
    COUNT(*) AS event_count
FROM raw_events
GROUP BY split(event, ',')[4];
```

### Course activity

```sql
SELECT
    split(event, ',')[3] AS course_id,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY split(event, ',')[3];
```

---

# 6. Apache HBase

HBase is used as a low-latency store for student activity alerts.

Table:

```text
student_alerts
```

Column family:

```text
activity
```

Example structure:

```text
student_alerts
│
├── student_id
│
└── activity
    ├── last_event_time
    └── status
```

This makes HBase suitable for retrieving the current state of a student's activity quickly.

---

# 🔄 End-to-End Data Flow

The complete pipeline works as follows:

```text
1. Student activity is generated
             ↓
2. Scala Producer creates event
             ↓
3. Producer sends event to Kafka
             ↓
4. Kafka partitions the event
             ↓
5. Kafka replicates the event
             ↓
6. Spark Structured Streaming reads event
             ↓
7. Spark processes the event
             ↓
8. Raw data is stored in HDFS
             ↓
9. Hive performs analytical queries
             ↓
10. HBase stores activity alerts
```

---

# 📊 Event Types

The producer generates six event types:

| Event             | Description                    |
| ----------------- | ------------------------------ |
| `LOGIN`           | Student logs into the platform |
| `VIDEO_START`     | Student starts a video         |
| `VIDEO_COMPLETE`  | Student completes a video      |
| `QUIZ_START`      | Student starts a quiz          |
| `QUIZ_SUBMIT`     | Student submits a quiz         |
| `COURSE_COMPLETE` | Student completes a course     |

---

# ⚙️ Prerequisites

Make sure the following are installed:

```text
Docker Desktop
WSL Ubuntu
Java
Scala
sbt
Hadoop
Hive
HBase
Spark
```

Verify the important versions:

```bash
java -version
scala -version
sbt --version
spark-submit --version
hadoop version
hive --version
```

Kafka is deployed using Docker.

---

# 🚀 How to Run the Project

## Step 1 — Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Move into the project:

```bash
cd online-education-monitoring
```

Open it in VS Code:

```bash
code .
```

---

# Step 2 — Start Kafka

From the project root:

```bash
docker compose up -d
```

Verify the three brokers:

```bash
docker ps
```

Expected:

```text
education-kafka-1
education-kafka-2
education-kafka-3
```

---

# Step 3 — Create Kafka Topic

Create the topic:

```bash
docker exec education-kafka-1 /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka1:19092 --topic education-events --partitions 3 --replication-factor 3
```

Configure minimum ISR:

```bash
docker exec education-kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:19092 --entity-type topics --entity-name education-events --alter --add-config min.insync.replicas=2
```

Verify:

```bash
docker exec education-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic education-events
```

Expected configuration:

```text
PartitionCount: 3
ReplicationFactor: 3
min.insync.replicas=2
```

---

# Step 4 — Start Hadoop/HDFS

Start the Hadoop services in WSL:

```bash
start-dfs.sh
```

Verify:

```bash
jps
```

Expected services include:

```text
NameNode
DataNode
SecondaryNameNode
```

Create the project HDFS directory:

```bash
hdfs dfs -mkdir -p /online-education/raw-events
```

---

# Step 5 — Start Spark Streaming

Open another VS Code terminal.

Go to the Spark project:

```bash
cd spark
```

Compile/run:

```bash
sbt run
```

Select:

```text
EducationActivityStreaming
```

Spark will now wait for Kafka events.

---

# Step 6 — Start the Producer

Open another VS Code terminal:

```bash
cd producer
```

Run:

```bash
sbt run
```

The producer continuously generates events.

Example output:

```text
Produced: key=S103 | E000001,S103,SESSION-S103-2,C101,LOGIN,...
Produced: key=S105 | E000002,S105,SESSION-S105-4,C102,VIDEO_START,...
Produced: key=S101 | E000003,S101,SESSION-S101-1,C103,QUIZ_START,...
```

Spark should simultaneously display the incoming events.

---

# Step 7 — Verify Kafka Messages

You can independently inspect Kafka:

```bash
docker exec education-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka1:19092 --topic education-events --from-beginning
```

This allows you to demonstrate that Kafka is receiving the producer events.

---

# Step 8 — Hive Analytics

Start Hive if it is not already running.

Execute:

```bash
hive
```

Then:

```sql
SOURCE hive/01_create_database.sql;
```

Create the external table:

```sql
SOURCE hive/02_create_raw_events.sql;
```

Run analytics:

```sql
SOURCE hive/03_analytics.sql;
```

Example results include:

```text
Total Events
Events per Student
Events per Event Type
Events per Course
Course Completion Counts
```

---

# Step 9 — HBase

Start HBase:

```bash
start-hbase.sh
```

Open HBase shell:

```bash
hbase shell
```

Create the alert table:

```text
create 'student_alerts', 'activity'
```

Verify:

```text
list
```

---

# 🛡️ Fault-Tolerance Demonstration

One of the important parts of this project is demonstrating Kafka fault tolerance.

The cluster has:

```text
3 Brokers
3 Partitions
Replication Factor = 3
Min ISR = 2
```

Stop Broker 3:

```bash
docker stop education-kafka-3
```

Check the topic:

```bash
docker exec education-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic education-events
```

Before failure:

```text
ISR: 1,2,3
```

After Broker 3 failure:

```text
ISR: 1,2
```

Kafka automatically elects new leaders where required.

The topic continues operating because the remaining two brokers satisfy:

```text
Min ISR = 2
```

Restart Broker 3:

```bash
docker start education-kafka-3
```

After replication catches up:

```text
ISR: 1,2,3
```

This demonstrates Kafka's:

* Replication
* Leader election
* ISR management
* Broker fault tolerance

---

# 🧪 Verification Checklist

After starting the project, verify each layer.

### Kafka

```bash
docker ps
```

Expected:

```text
education-kafka-1
education-kafka-2
education-kafka-3
```

### Topic

```bash
docker exec education-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic education-events
```

Expected:

```text
Partitions = 3
Replication Factor = 3
Min ISR = 2
```

### Producer

Expected:

```text
Produced: key=S101 | ...
```

### Spark

Expected:

```text
Batch: 0
Batch: 1
Batch: 2
...
```

with incoming education events.

### HDFS

```bash
hdfs dfs -ls /online-education/raw-events
```

### Hive

```sql
SELECT COUNT(*) FROM raw_events;
```

### HBase

```text
list
```

Expected:

```text
student_alerts
```

---

# 🧠 Key Data Engineering Concepts Demonstrated

## Kafka Partitioning

Events are distributed across three partitions.

The producer uses:

```text
student_id
```

as the Kafka message key.

This provides deterministic partition assignment for a given key while the partition count remains unchanged.

---

## Kafka Replication

Each partition has three replicas.

```text
Partition 0 → Broker 1 + Broker 2 + Broker 3
Partition 1 → Broker 1 + Broker 2 + Broker 3
Partition 2 → Broker 1 + Broker 2 + Broker 3
```

This provides redundancy.

---

## ISR

ISR means:

> In-Sync Replicas

Initially:

```text
ISR = 1,2,3
```

After Broker 3 failure:

```text
ISR = 1,2
```

Because:

```text
min.insync.replicas = 2
```

the topic still has the required number of synchronized replicas.

---

## Spark Structured Streaming

Spark uses:

```scala
readStream
```

to continuously consume Kafka data.

Instead of processing one large static dataset, Spark processes incoming data as streaming micro-batches.

---

## HDFS

HDFS provides distributed persistent storage for the raw event data.

This separates real-time processing from long-term storage and analytics.

---

## Hive

Hive provides a SQL interface for analytical processing over stored data.

This allows questions such as:

```text
How many events occurred?

Which students are most active?

Which courses have the most activity?

How many students completed a course?
```

---

## HBase

HBase provides low-latency access to student activity state and alerts.

It is appropriate for workloads requiring fast row-level access rather than large analytical scans.

---
