#!/bin/bash

export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}
export SPARK_HOME=/spark

/wait-for-step.sh
/execute-step.sh

if [ ! -z "${SPARK_APPLICATION_JAR_LOCATION}" ]; then
    echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
    echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
    /${SPARK_HOME}/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2  \
        --class ${SPARK_APPLICATION_MAIN_CLASS} \
        --master ${SPARK_MASTER_URL} \
        --conf "spark.driver.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
        --conf "spark.executor.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
        ${SPARK_SUBMIT_ARGS} \
        ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}
else
    if [ ! -z "${SPARK_APPLICATION_PYTHON_LOCATION}" ]; then
        echo "Submit application ${SPARK_APPLICATION_PYTHON_LOCATION} to Spark master ${SPARK_MASTER_URL}"
        echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
        PYSPARK_PYTHON=python3  /spark/bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2 \
            --master ${SPARK_MASTER_URL} \
            --conf "spark.driver.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
            --conf "spark.executor.extraJavaOptions=-Dkafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
            ${SPARK_SUBMIT_ARGS} \
            ${SPARK_APPLICATION_PYTHON_LOCATION} ${SPARK_APPLICATION_ARGS} \
    else
        echo "Not recognized application."
    fi
fi

/finish-step.sh
