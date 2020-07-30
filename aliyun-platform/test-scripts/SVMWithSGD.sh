hdfs dfs -rm -r hdfs://bach001:9000/user/root/target/tmp/scalaSVMWithSGDModel

appName="SVMWithSGDExample"
inputData="hdfs://bach001:9000/user/root/data/mllib/sample_libsvm_data.txt"
numIterations=10
trainRate=0.9

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.mllib.SVMWithSGDExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.eventLog.enabled=true \
    --driver-memory 4g \
    --num-executors 6 \
    --executor-memory 3g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar 
