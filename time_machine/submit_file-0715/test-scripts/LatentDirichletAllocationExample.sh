hdfs dfs -rm -r hdfs://bach001:9000/user/root/target/org/apache/spark/LatentDirichletAllocationExample

#appName="LatentDirichletAllocationExample"
#inputData="hdfs://bach001:9000/user/root/data/mllib/sample_lda_data.txt"
#numIterations=10
#trainRate=0.9

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.mllib.LatentDirichletAllocationExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.eventLog.enabled=true \
    --driver-memory 4g \
    --num-executors 6 \
    --executor-memory 3g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar
