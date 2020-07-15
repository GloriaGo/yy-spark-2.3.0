hdfs dfs -rm -r target/tmp/scalaSVMwithMASGDModel

inputData="data/kdd12s/repar_kdd12_0"
numIterations=2
appName="kdd12_0.5G"
stepSize=1.0
regParam=0.01
miniBatchFraction=1.0

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.mllib.SVMWithMASGDExample \
    --master yarn-client \
    --conf spark.eventLog.enabled=true \
    --conf spark.locality.wait=1s \
    --conf spark.driver.memory=6g \
    --conf spark.driver.cores=1 \
    --conf spark.sql.files.maxPartitionBytes=536870912 \
    --conf spark.driver.maxResultSize=10g \
    --conf spark.rpc.message.maxSize=2000 \
    --conf spark.shuffle.file.buffer=64m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=2000m \
    --conf spark.memory.fraction=0.9 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.heartbeatInterval=5s \
    --conf spark.executorextraJavaOptions=-XX:+UseG1GC \
    --num-executors 3 \
    --executor-memory 6g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    $appName \
    $inputData \
    $stepSize \
    $regParam \
    $miniBatchFraction \
    $numIterations
