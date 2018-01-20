#!/bin/sh

base_dir=$(cd `dirname $0`;pwd)

LIB_DIR=/usr/local/spark-2.2.0/jars

MAIN_CLASS=com.run.dsf.spark.streaming.DSFKafkaStreaming
${LIB_DIR}/../bin/spark-submit --master spark://Master1.Hadoop:7077 --executor-memory 2G --executor-cores 4  --class ${MAIN_CLASS}   /tmp/DataStorage.jar


$SPARK_HOME/bin/spark-submit \
--master spark://192.168.42.24:7077 \
--class main.sparkStreaming.startStreamingTest /home/hadoop/SparkstartBatch.jar \


#!/bin/sh

base_dir=$(cd `dirname $0`;pwd)

LIB_DIR=/usr/local/spark-2.2.0/jars

MAIN_CLASS=HttpNetLog.SparkStart
${LIB_DIR}/../bin/spark-submit --master spark://Master1.Hadoop:7077 --executor-memory 2G --executor-cores 4  --class ${MAIN_CLASS} /home/hadoop/SparkPractise.jar

#nohup ${LIB_DIR}/../bin/spark-submit --master spark://hadoop2:7077 --executor-memory 4G --executor-cores 1 --class ${MAIN_CLASS} ${base_dir}/nssa_spark.jar >/dev/null 2>&1 &
