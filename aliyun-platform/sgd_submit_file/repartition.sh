rawData="data/1.0_kdd12"
reparData="data/repar_1.0_kdd12"
executorSize=8

hdfs dfs -rm -r $reparData

$SPARK_HOME/bin/spark-submit  --master yarn \
    --class org.apache.spark.examples.ml.JavaRepartitionDataset \
    --conf spark.locality.wait=0s \
    --conf spark.sql.files.maxPartitionBytes=3000000000 \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.rpc.message.maxSize=2000 \
    --conf spark.shuffle.file.buffer=64m \
    --conf spark.broadcast.blocksize=32m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.memory.fraction=0.5 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.heartbeatInterval=100s \
    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
    --conf spark.executor.extraJavaOptions=-XX:+UseCompressedOops \
    --conf spark.rdd.compress=true \
    --conf spark.cleaner.periodicGC.interval=10min \
    --num-executors $executorSize \
    --executor-memory 10g \
    --executor-cores 1 \
    --driver-memory 10g \
    --driver-cores 1\
    --driver-java-options -XX:+UseG1GC \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    $rawData \
    $reparData \
    16
