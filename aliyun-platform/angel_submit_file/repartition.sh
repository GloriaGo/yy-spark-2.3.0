rawData="data/test_pubmed_libsvm.txt"
reparData="data/repar_test_pubmed_libsvm"
executorSize=8

hdfs dfs -rm -r $reparData

$SPARK_HOME/bin/spark-submit  --master yarn \
    --class org.apache.spark.examples.ml.JavaRepartitionDataset \
    --conf spark.eventLog.enabled=true \
    --conf spark.locality.wait=1s \
    --conf spark.driver.memory=12g \
    --conf spark.driver.cores=1 \
    --conf spark.sql.files.maxPartitionBytes=536870912 \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.rpc.message.maxSize=2000 \
    --conf spark.shuffle.file.buffer=64m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.memory.fraction=0.9 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.heartbeatInterval=5s \
    --conf spark.executorextraJavaOptions=-XX:+UseG1GC \
    --num-executors $executorSize \
    --executor-memory 12g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    $rawData \
    $reparData \
    $executorSize
