$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi \
    --master yarn-client \
    --conf spark.locality.wait=1s \
    --driver-memory 4g \
    --num-executors 6 \
    --executor-memory 3g \
    --executor-cores 1 \
    --queue default \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    1000
