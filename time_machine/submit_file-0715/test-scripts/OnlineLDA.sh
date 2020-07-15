hdfs dfs -rm -r hdfs://bach001:9000/user/root/target/org/apache/spark/LatentDirichletAllocationExample

#appName="LatentDirichletAllocationExample"
inputData="hdfs://bach001:9000/user/root/data/mllib/sample_lda_data.txt"
maxIter=10
k=5

$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.ml.JavaLDAExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.eventLog.enabled=true \
    --driver-memory 4g \
    --num-executors 6 \
    --executor-memory 3g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar 
    #$inputData \
    #$k \
    #$maxIter
