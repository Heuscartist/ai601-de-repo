Additional Details about the Assignment

## Files

producer.py: Based off the music_producer.py file from lab5. Creates a kafka topic and generates random data about traffic sensors and transmits them publishes them also starts prometheus client and sends data to grafana through prometheus

streaming.py: Based off the now_trending.py file from lab5. Streams data using Spark Structured Streaming. Several queries are run for various queries such as average speed, top sensors by car volume, congestion levels etc.

zk-single-kafka.yml: modified for prometheus

## Folders

images: contains screenshots from the terminal through streaming.py and from grafana dashboard. Also contains architecture diagram.

jmx-exporter: for prometheus



## How to Run

### COMMON

- activate environment
- install requirements

docker-compose -f zk-single-kafka.yml up -d
docker-compose -f zk-single-kafka.yml ps
docker exec -it kafka1 /bin/bash
unset KAFKA_OPTS
kafka-topics --create --topic traffic_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --list --bootstrap-server localhost:9092
exit

python producer.py


### FOR GRAFANA
Go to http://localhost:3000 (default: admin/admin)
Configuration > Data Sources > Add Prometheus (if not already done)
URL: http://prometheus:9090
Save & Test
Add Queries for Visualization


### FOR PYSTRUCTURED STREAMING
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 streaming.py