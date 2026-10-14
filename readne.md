```markdown
# Wikimedia Real-Time Data Pipeline

A real-time streaming pipeline that ingests Wikimedia's recent change events, publishes them to Kafka, and consumes them with multiple aggregation consumers. Orchestrated with Airflow, containerized with Docker Compose.

## Technologies

- **Python** — producer and consumer scripts
- **aiohttp** — async HTTP client for streaming the SSE feed
- **Apache Kafka** — message broker for the event stream
- **Apache Airflow** — orchestration and hourly health check
- **PostgreSQL** — Airflow metadata database
- **Docker Compose** — runs Kafka, Postgres, and Airflow together
- **Matplotlib** — charts generated from the consumer output

## What it does

1. A producer connects to Wikimedia's `recentchange` SSE stream.
2. Each event is parsed, enriched, and published to the Kafka topic `eqiad.mediawiki.recentchange`.
3. Consumers read from the topic and aggregate: bot vs human activity, edits per wiki, edits per title.
4. Results are printed live and appended to `data.txt`.
5. `plot.py` reads `data.txt` and generates `bots_vs_humans.png`.
6. An Airflow DAG (`health_check`) runs hourly to verify the producer is up.

## How to run

**1. Start the stack**

```bash
sudo chown -R 50000:0 ./logs ./dags ./plugins && docker compose down -v && docker compose up -d && sleep 30 && docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic eqiad.mediawiki.recentchange --partitions 1 --replication-factor 1 --if-not-exists && docker exec airflow airflow dags list
```

**2. Start the producer** (one terminal)

```bash
uv run dags/producer.py
```

**3. Start the consumer** (another terminal)

```bash
uv run consumer.py
```

**4. Generate the chart**

```bash
uv run plot.py
```

**5. Open Airflow**

```
http://localhost:8080
username: admin
password: admin
```

## Files

```
producer.py       – streams from Wikimedia, publishes to Kafka
consumer.py       – reads from Kafka, aggregates, writes data.txt
plot.py           – reads data.txt, generates bots_vs_humans.png
dags/
  airflow_dag.py  – hourly health check DAG
  producer.py     – copy of producer used by the DAG
docker-compose.yml
data.txt
bots_vs_humans.png
```

## Sample output

```
bot=2129 | human=8698 | wiki={'event_wiki': 'commonswiki', 'count: ': 6280} | title={'event_title': 'Category:Images from Mapillary', 'count: ': 48}
```

## Notes

- The producer runs as a host process. Kafka, Postgres, and Airflow run in Docker.
- `data.txt` is append-only. Delete it before a fresh run if you want a clean chart.
- The Airflow DAG only checks producer health — it does not run the producer itself.
