#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

if [[ -z "$SENSITIVE_PROPERTIES" ]]; then
    SENSITIVE_PROPERTIES="CONNECT_SASL_JAAS_CONFIG,CONNECT_CONSUMER_SASL_JAAS_CONFIG,CONNECT_PRODUCER_SASL_JAAS_CONFIG,CONNECT_SSL_KEYSTORE_PASSWORD,CONNECT_PRODUCER_SSL_KEYSTORE_PASSWORD,CONNECT_SSL_TRUSTSTORE_PASSWORD,CONNECT_PRODUCER_SSL_TRUSTSTORE_PASSWORD,CONNECT_SSL_KEY_PASSWORD,CONNECT_PRODUCER_SSL_KEY_PASSWORD,CONNECT_CONSUMER_SSL_TRUSTSTORE_PASSWORD,CONNECT_CONSUMER_SSL_KEYSTORE_PASSWORD,CONNECT_CONSUMER_SSL_KEY_PASSWORD"
fi

if [[ -z "$BOOTSTRAP_SERVERS" ]]; then
    # Look for any environment variables set by Docker container linking. For example, if the container
    # running Kafka were aliased to 'kafka' in this container, then Docker should have created several envs,
    # such as 'KAFKA_PORT_9092_TCP'. If so, then use that to automatically set the 'bootstrap.servers' property.
    BOOTSTRAP_SERVERS=$(env | grep .*PORT_9092_TCP= | sed -e 's|.*tcp://||' | uniq | paste -sd ,)
fi

if [[ "x$BOOTSTRAP_SERVERS" = "x" ]]; then
    export BOOTSTRAP_SERVERS=0.0.0.0:9092
fi

echo "Using BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS"


if [[ -z "$HOST_NAME" ]]; then
    HOST_NAME=$(ip addr | grep 'BROADCAST' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
fi

: ${REST_PORT:=8083}
: ${REST_HOST_NAME:=$HOST_NAME}
: ${ADVERTISED_PORT:=8083}
: ${ADVERTISED_HOST_NAME:=$HOST_NAME}
: ${GROUP_ID:=1}
: ${OFFSET_FLUSH_INTERVAL_MS:=60000}
: ${OFFSET_FLUSH_TIMEOUT_MS:=5000}
: ${SHUTDOWN_TIMEOUT:=10000}
: ${KEY_CONVERTER:=org.apache.kafka.connect.json.JsonConverter}
: ${VALUE_CONVERTER:=org.apache.kafka.connect.json.JsonConverter}
: ${ENABLE_APICURIO_CONVERTERS:=false}
: ${ENABLE_DEBEZIUM_SCRIPTING:=false}
export CONNECT_REST_ADVERTISED_PORT=$ADVERTISED_PORT
export CONNECT_REST_ADVERTISED_HOST_NAME=$ADVERTISED_HOST_NAME
export CONNECT_REST_PORT=$REST_PORT
export CONNECT_REST_HOST_NAME=$REST_HOST_NAME
export CONNECT_BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS
export CONNECT_GROUP_ID=$GROUP_ID
export CONNECT_CONFIG_STORAGE_TOPIC=$CONFIG_STORAGE_TOPIC
export CONNECT_OFFSET_STORAGE_TOPIC=$OFFSET_STORAGE_TOPIC
if [[ -n "$STATUS_STORAGE_TOPIC" ]]; then
    export CONNECT_STATUS_STORAGE_TOPIC=$STATUS_STORAGE_TOPIC
fi
export CONNECT_KEY_CONVERTER=$KEY_CONVERTER
export CONNECT_VALUE_CONVERTER=$VALUE_CONVERTER
export CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS=$SHUTDOWN_TIMEOUT
export CONNECT_OFFSET_FLUSH_INTERVAL_MS=$OFFSET_FLUSH_INTERVAL_MS
export CONNECT_OFFSET_FLUSH_TIMEOUT_MS=$OFFSET_FLUSH_TIMEOUT_MS
if [[ -n "$HEAP_OPTS" ]]; then
    export KAFKA_HEAP_OPTS=$HEAP_OPTS
fi
unset HOST_NAME
unset REST_PORT
unset REST_HOST_NAME
unset ADVERTISED_PORT
unset ADVERTISED_HOST_NAME
unset GROUP_ID
unset OFFSET_FLUSH_INTERVAL_MS
unset OFFSET_FLUSH_TIMEOUT_MS
unset SHUTDOWN_TIMEOUT
unset KEY_CONVERTER
unset VALUE_CONVERTER
unset HEAP_OPTS
unset MD5HASH
unset SCALA_VERSION

#
# Set up the classpath with all the plugins ...
#
if [ -z "$CONNECT_PLUGIN_PATH" ]; then
    CONNECT_PLUGIN_PATH=$KAFKA_CONNECT_PLUGINS_DIR
fi
echo "Plugins are loaded from $CONNECT_PLUGIN_PATH"

if [[ "${ENABLE_APICURIO_CONVERTERS}" == "true" && ! -z "$EXTERNAL_LIBS_DIR" && -d "$EXTERNAL_LIBS_DIR/apicurio" ]] ; then
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        for connector in $plugin_dir/*/ ; do
            ln -snf $EXTERNAL_LIBS_DIR/apicurio/* "$connector"
        done
    done
    echo "Apicurio connectors enabled!"
else
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        find $plugin_dir/ -lname "$EXTERNAL_LIBS_DIR/apicurio/*" -exec rm -f {} \;
    done
fi

if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" && ! -f "$EXTERNAL_LIBS_DIR" && -d "$EXTERNAL_LIBS_DIR/debezium-scripting" ]] ; then
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        for connector in $plugin_dir/*/ ; do
            ln -snf $EXTERNAL_LIBS_DIR/debezium-scripting/*.jar "$connector"
        done
    done
    echo "Debezium Scripting enabled!"
else
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        find $plugin_dir/ -lname "$EXTERNAL_LIBS_DIR/debezium-scripting/*" -exec rm -f {} \;
    done
fi

#
# Set up the JMX options
#
: ${JMXAUTH:="false"}
: ${JMXSSL:="false"}
if [[ -n "$JMXPORT" && -n "$JMXHOST" ]]; then
    echo "Enabling JMX on ${JMXHOST}:${JMXPORT}"
    export KAFKA_JMX_OPTS="-Djava.rmi.server.hostname=${JMXHOST} -Dcom.sun.management.jmxremote.rmi.port=${JMXPORT} -Dcom.sun.management.jmxremote.port=${JMXPORT} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=${JMXAUTH} -Dcom.sun.management.jmxremote.ssl=${JMXSSL} "
fi

#
# Setup Flight Recorder
#
if [[ "$ENABLE_JFR" == "true" ]]; then
    JFR_OPTS="-XX:StartFlightRecording"
    opt_delimiter="="
    for VAR in $(env); do
      if [[ "$VAR" == JFR_RECORDING_* ]]; then
        opt_name=`echo "$VAR" | sed -r "s/^JFR_RECORDING_([^=]*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ -`
        opt_value=`echo "$VAR" | sed -r "s/^JFR_RECORDING_[^=]*=(.*)/\1/g"`
        JFR_OPTS="${JFR_OPTS}${opt_delimiter}${opt_name}=${opt_value}"
        opt_delimiter=","
      fi
    done
    opt_delimiter=" -XX:FlightRecorderOptions="
    for VAR in $(env); do
      if [[ "$VAR" == JFR_OPT_* ]]; then
        opt_name=`echo "$VAR" | sed -r "s/^JFR_OPT_([^=]*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ -`
        opt_value=`echo "$VAR" | sed -r "s/^JFR_OPT_[^=]*=(.*)/\1/g"`
        JFR_OPTS="${JFR_OPTS}${opt_delimiter}${opt_name}=${opt_value}"
        opt_delimiter=","
      fi
    done
    echo "Java Flight Recorder enabled and configured with options $JFR_OPTS"
    if [[ -n "$KAFKA_OPTS" ]]; then
        export KAFKA_OPTS="$KAFKA_OPTS $JFR_OPTS"
    else
        export KAFKA_OPTS="$JFR_OPTS"
    fi
    unset JFR_OPTS
fi

#
# Setup Kafka Prometheus Metrics
#
if [ "$ENABLE_JMX_EXPORTER" = "true" ]; then
  KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/config/metrics.yaml"
  export KAFKA_OPTS
fi

#
# Make sure the directory for logs exists ...
#
mkdir -p ${KAFKA_DATA}/$KAFKA_BROKER_ID

# Process the argument to this container ...
case $1 in
    start)
        if [[ "x$CONNECT_BOOTSTRAP_SERVERS" = "x" ]]; then
            echo "The BOOTSTRAP_SERVERS variable must be set, or the container must be linked to one that runs Kafka."
            exit 1
        fi

        if [[ "x$CONNECT_GROUP_ID" = "x" ]]; then
            echo "The GROUP_ID must be set to an ID that uniquely identifies the Kafka Connect cluster these workers belong to."
            echo "Ensure this is unique for all groups that work with a Kafka cluster."
            exit 1
        fi

        if [[ "x$CONNECT_CONFIG_STORAGE_TOPIC" = "x" ]]; then
            echo "The CONFIG_STORAGE_TOPIC variable must be set to the name of the topic where connector configurations will be stored."
            echo "This topic must have a single partition, be highly replicated (e.g., 3x or more) and should be configured for compaction."
            exit 1
        fi

        if [[ "x$CONNECT_OFFSET_STORAGE_TOPIC" = "x" ]]; then
            echo "The OFFSET_STORAGE_TOPIC variable must be set to the name of the topic where connector offsets will be stored."
            echo "This topic should have many partitions (e.g., 25 or 50), be highly replicated (e.g., 3x or more) and be configured for compaction."
            exit 1
        fi

        if [[ "x$CONNECT_STATUS_STORAGE_TOPIC" = "x" ]]; then
            echo "WARNING: it is recommended to specify the STATUS_STORAGE_TOPIC variable for defining the name of the topic where connector statuses will be stored."
            echo "This topic may have multiple partitions, be highly replicated (e.g., 3x or more) and should be configured for compaction."
            echo "As no value is given, the default of 'connect-status' will be used."
        fi

        echo "Here we are"
        cat $KAFKA_HOME/config/connect-distributed.properties
        #
        # Execute the Kafka Connect distributed service, replacing this shell process with the specified program ...
        #
        exec $KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties
        ;;
esac

# Otherwise just run the specified command
exec "$@"