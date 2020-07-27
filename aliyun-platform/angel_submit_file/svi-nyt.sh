#!/bin/bash
rm /root/YY-logs/YY-spark.log
for slave in $(cat slave_list)
do
	ssh $slave "rm /root/YY-logs/YY-spark.log"
done

source $ANGEL_HOME/bin/spark-on-angel-env.sh

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --conf spark.yarn.queue=default \
    --conf spark.locality.wait=3s \
    --conf spark.sql.files.maxPartitionBytes=737003200 \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.rpc.message.maxSize=512 \
    --conf spark.shuffle.file.buffer=64m \
    --conf spark.broadcast.blocksize=32m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.memory.fraction=0.7 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.heartbeatInterval=5s \
    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
    --conf spark.cleaner.periodicGC.interval=10min \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.am.memory=7g \
    --conf spark.ps.instances=8 \
    --conf spark.ps.cores=1 \
    --conf spark.ps.memory=7g \
    --conf spark.ps.log.level=INFO \
    --conf spark.log.path="/root/YY-logs/angel-logs" \
    --jars $SONA_SPARK_JARS\
    --name "LDA-spark-on-angel" \
    --num-executors  8 \
    --executor-cores 1 \
    --executor-memory 8g \
    --driver-cores 1 \
    --driver-memory 7g \
    --driver-java-options -XX:+UseG1GC \
    --class com.tencent.angel.spark.examples.basic.BRAROnlineLDA \
    $ANGEL_HOME/lib/spark-on-angel-examples-${ANGEL_VERSION}.jar \
    input:"hdfs://bach101:9000/user/root/data/repar_nytimes_libsvm" \
    test:"hdfs://bach101:9000/user/root/data/repar_test_nytimes_libsvm" \
    numIter:301 \
    topicSize:200 \
    miniBatchFraction:0.0034 \
    kappa:0.51 \
    tau0:64
