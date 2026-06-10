# 📊 Kafka Log Pipeline

Real-time log processing pipeline built with **Java 21**, **Spring Boot 3** and **Kafka Streams**.  
Simulates application logs being published every 500ms, routes them by severity using stream processing, and triggers alerts when a service exceeds a configurable error threshold within a tumbling time window.

---

## 🏗️ Architecture

```
[LogProducer — @Scheduled 500ms]
         │
         │ publishes: LogEvent (INFO / WARN / ERROR)
         ▼
[Topic: raw-logs]
         │
         ▼
[LogStreamTopology — Kafka Streams]
         │
         ├──► [Topic: info-logs]
         ├──► [Topic: warn-logs]
         └──► [Topic: error-logs]
                      │
                      ▼
         [AlertStreamTopology — Kafka Streams]
         │  Tumbling window: 60s
         │  Groups by service name
         │  Counts errors per window
         │  If count >= threshold → publish alert
                      │
                      ▼
              [Topic: alerts]
                      │
                      ▼
              [AlertConsumer]
              Logs alert (extensible to Slack / PagerDuty)
```

---

## 🛠️ Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 21 | Language |
| Spring Boot | 3.5 | Framework |
| Kafka Streams | 3.9 | Stream processing |
| Spring Kafka | 3.3 | Kafka integration |
| Docker Compose | - | Local infrastructure |
| Kafdrop | latest | Kafka UI |
| JUnit 5 + Mockito | - | Unit tests |
| TopologyTestDriver | - | Streams unit tests (no broker needed) |
| EmbeddedKafka | - | Integration tests |
| Awaitility | - | Async test assertions |

---

## ▶️ Running Locally

### Prerequisites

- Docker Desktop
- Java 21 (Temurin recommended)
- Maven

### Steps

**1. Start infrastructure**
```bash
docker-compose up -d
```

**2. Run the application**
```bash
./mvnw spring-boot:run
```

**3. Watch logs being produced and routed**

Open Kafdrop at **http://localhost:9000** and watch messages arriving in `raw-logs`, being split into `info-logs`, `warn-logs`, `error-logs`, and alerts appearing in `alerts` when thresholds are exceeded.

**4. Run tests**
```bash
./mvnw test
```

---

## ⚙️ Configuration

```yaml
app:
  kafka:
    alert:
      error-threshold: 10       # how many errors trigger an alert
      window-size-seconds: 60   # tumbling window size
```

Set `error-threshold: 3` for quick testing — alerts will fire within the first minute.

---

## 🔄 Event Flow

1. `LogProducer` publishes a `LogEvent` to `raw-logs` every **500ms** with a random severity (60% INFO, 25% WARN, 15% ERROR)
2. `LogStreamTopology` consumes `raw-logs` and routes each event to `info-logs`, `warn-logs`, or `error-logs` based on level
3. `AlertStreamTopology` consumes `error-logs`, groups by service name, and counts errors in a **60-second tumbling window**
4. When the count exceeds the threshold, an `AlertEvent` is published to `alerts`
5. `AlertConsumer` receives the alert and logs it — ready to be extended with Slack, PagerDuty, or email

---

## 💡 Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Kafka Streams DSL | `LogStreamTopology` — filter, split, branch, to |
| Tumbling window | `AlertStreamTopology` — `TimeWindows.ofSizeWithNoGrace` |
| Stateful aggregation | `count()` with `Materialized` store |
| Multi-topic routing | `raw-logs` → `info-logs` / `warn-logs` / `error-logs` |
| Threshold-based alerting | `filter()` after `count()` |
| Custom Serde | `logEventSerde` and `alertEventSerde` for JSON serialization |
| Scheduled producer | `@Scheduled(fixedDelay = 500)` |
| TopologyTestDriver | Unit tests with no Kafka broker |
| EmbeddedKafka | Integration tests |

---

## 🗂️ Project Structure

```
src/
├── main/java/com/zs/kafka_log_pipeline/
│   ├── config/
│   │   └── KafkaStreamsConfig.java       # @EnableKafkaStreams + custom Serdes
│   ├── consumer/
│   │   └── AlertConsumer.java            # Consumes alerts topic
│   ├── event/
│   │   ├── LogLevel.java                 # INFO, WARN, ERROR
│   │   ├── LogEvent.java                 # record: id, level, service, message, timestamp
│   │   └── AlertEvent.java               # record: service, errorCount, threshold, triggeredAt
│   ├── producer/
│   │   └── LogProducer.java              # @Scheduled — publishes to raw-logs every 500ms
│   └── streams/
│       ├── LogStreamTopology.java        # Routes logs by severity
│       └── AlertStreamTopology.java      # Counts errors in window, triggers alerts
└── test/java/com/zs/kafka_log_pipeline/
    ├── streams/
    │   └── LogStreamTopologyTest.java    # TopologyTestDriver — no broker needed
    ├── producer/
    │   └── LogProducerTest.java          # Unit tests with Mockito
    └── consumer/
        └── AlertConsumerIntegrationTest  # EmbeddedKafka integration test
```

---

## 🧪 Tests

| Test | Type | What it covers |
|---|---|---|
| `LogStreamTopologyTest` | Unit (TopologyTestDriver) | INFO → info-logs, WARN → warn-logs, ERROR → error-logs, null filter |
| `LogProducerTest` | Unit | Publishes to correct topic, all fields populated, valid log levels |
| `AlertConsumerIntegrationTest` | Integration (EmbeddedKafka) | Consumer receives and processes alert event |
