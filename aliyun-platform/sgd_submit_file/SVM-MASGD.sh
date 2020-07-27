#hdfs dfs -rm -r target/tmp/scalaSVMWithMASGDModel/metadata
#hdfs dfs -rm -r target/tmp/scalaSVMWithMASGDModel
rm /root/YY-logs/YY-spark.log
for slave in $(cat slave_list)
do
	ssh $slave "rm /root/YY-logs/YY-spark.log"
done

inputData="data/repar_0.6_kdd12"
numIterations=5
appName="kdd12"
stepSize=0.01
regParam=0.0001
miniBatchFraction=1.0

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.mllib.SVMWithMASGDExample \
    --master yarn-client \
    --conf spark.eventLog.enabled=true \
    --conf spark.locality.wait=0s \
    --conf spark.files.maxPartitionBytes=3g \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.rpc.message.maxSize=2000 \
    --conf spark.shuffle.file.buffer=128m \
    --conf spark.broadcast.blocksize=32m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.memory.fraction=0.9 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.heartbeatInterval=10s \
    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
    --conf spark.cleaner.periodicGC.interval=10min \
    --num-executors 8 \
    --executor-memory 10g \
    --executor-cores 1 \
    --driver-memory 8g \
    --driver-cores 1\
    --driver-java-options -XX:+UseG1GC \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    $appName \
    $inputData \
    $stepSize \
    $regParam \
    $miniBatchFraction \
    $numIterations \
    8
