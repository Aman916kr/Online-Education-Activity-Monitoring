# Online Education Activity Monitoring

A real-time data engineering project for monitoring student activity
on an online learning platform.

## Architecture

Application
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
HDFS
    ↓
Hive

HBase is used for real-time student activity alerts.

## Events

- LOGIN
- VIDEO_START
- VIDEO_COMPLETE
- QUIZ_START
- QUIZ_SUBMIT
- COURSE_COMPLETE

## Technologies

- Scala
- Apache Kafka
- Apache Spark
- Hadoop HDFS
- Apache Hive
- Apache HBase

## Planned Analytics

- Active students
- Video completion rate
- Quiz completion rate
- Course completion rate
- Inactive students
- Incomplete learning journeys

## Project Status

- [x] Step 1 — Project structure
- [ ] Step 2 — Kafka topic
- [ ] Step 3 — Scala producer
- [ ] Step 4 — Kafka consumer test
- [ ] Step 5 — Spark Structured Streaming
- [ ] Step 6 — HDFS raw storage
- [ ] Step 7 — Active-student calculation
- [ ] Step 8 — Completion rates
- [ ] Step 9 — Stateful inactivity detection
- [ ] Step 10 — Hive tables + analytics
- [ ] Step 11 — HBase alerts
- [ ] Step 12 — 3-broker replication
- [ ] Step 13 — Fault-tolerance testing
- [ ] Step 14 — Final documentation