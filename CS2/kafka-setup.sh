bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# See how many partitions in a topic
bin/kafka-topics.sh --describe --topic unzip-topic --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic zip-topic --bootstrap-server localhost:9092

# Change a topic's partition number
bin/kafka-topics.sh --alter --topic unzip-topic --partitions 4 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic ocr-topic --partitions 4 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic translate-topic --partitions 4 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic pdf-topic --partitions 4 --bootstrap-server localhost:9092
bin/kafka-topics.sh --alter --topic zip-topic --partitions 4 --bootstrap-server localhost:9092


bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ocr-topic --from-beginning