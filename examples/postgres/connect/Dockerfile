FROM quay.io/debezium/connect:2.0

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY connect-distributed.properties /kafka/config/

ENV KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/kafka/config/log4j.properties
ENV CONFIG_STORAGE_TOPIC=my_connect_configs
ENV OFFSET_STORAGE_TOPIC=my_connect_offsets
ENV STATUS_STORAGE_TOPIC=my_connect_statuses

ENTRYPOINT ["/docker-entrypoint.sh", "start"]
