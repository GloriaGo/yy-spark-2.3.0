$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn-client \
    --conf spark.locality.wait=0s \
    --driver-memory 15g \
    --num-executors 4 \
    --executor-memory 13g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    1000
