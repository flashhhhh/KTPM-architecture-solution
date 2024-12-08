bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# See how many partitions in a topic
bin/kafka-topics.sh --describe --topic unzip-topic --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic ocr-topic --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic translate-topic --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic pdf-topic --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic zip-topic --bootstrap-server localhost:9092

# Change a topic's partition number
bin/kafka-topics.sh --alter --topic unzip-topic --partitions 4 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic ocr-topic --partitions 8 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic translate-topic --partitions 2 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic pdf-topic --partitions 2 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic zip-topic --partitions 4 --bootstrap-server localhost:9092

# List all topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic zip-topic --from-beginning

# Create a topic
bin/kafka-topics.sh --create --topic unzip-topic --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic ocr-topic --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic translate-topic --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic pdf-topic --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic zip-topic --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

# Consume message in a partition
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ocr-topic --from-beginning --partition 0

# Delete messages in a topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic unzip-topic --delete
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic ocr-topic --delete
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic translate-topic --delete
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic pdf-topic --delete
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic zip-topic --delete