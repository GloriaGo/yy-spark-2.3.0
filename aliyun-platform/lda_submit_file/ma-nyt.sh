rm /root/YY-logs/YY-spark.log
for slave in $(cat slave_list)
do	
	ssh $slave "rm /root/YY-logs/YY-spark.log"
done

trainData="data/repar_nytimes_libsvm"
testData="data/repar_test_nytimes_libsvm"
numIterations=51
topicSize=200
appName="ma_nytimes_64k"
kappa=0.9
tau0=1024
miniBatchFraction=0.05
workerSize=8

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.ml.JavaMALDAExample \
    --master yarn-client \
    --conf spark.eventLog.enabled=true \
    --conf spark.locality.wait=1s \
    --conf spark.sql.files.maxPartitionBytes=734003200 \
    --conf spark.driver.maxResultSize=2g \
    --conf spark.rpc.message.maxSize=256 \
    --conf spark.shuffle.file.buffer=64m \
    --conf spark.broadcast.blocksize=32m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.memory.fraction=0.7 \
    --conf spark.task.cpus=1 \
    --conf spark.executor.heartbeatInterval=5s \
    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
    --conf spark.cleaner.periodicGC.interval=10min \
    --num-executors $workerSize \
    --driver-memory 10g \
    --driver-cores 1\
    --executor-memory 12g \
    --executor-cores 1 \
    --driver-java-options -XX:+UseG1GC \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    $appName \
    $trainData \
    $testData \
    $numIterations \
    $topicSize \
    $miniBatchFraction \
    $kappa \
    $tau0 \
    $workerSize
