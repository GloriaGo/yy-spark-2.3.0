# Apache Spark with Model Average

Spark is a fast and general cluster computing system for Big Data.
It provides MLlib for machine learning but slow.
Model averaging could significantly improve the performance of MLlib.
In this project, we take SGD and LDA as examples to show the MA-based versions are faster.

## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions for Spark-2.3.0.

## Building Spark with MA-based MLlib

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark with hadoop-2.7.7 and its example programs, run:

    build/mvn -e -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.7 -Dmaven.test.skip=true clean package

To build MLlib only, run:

    build/mvn -e -pl mllib clean package -Dmaven.test.skip=true -T 4

If you have deployed the official release version of Spark, you need to replace the original jars in the pre-build package with the new ones:

    cp examples/target/original-spark-examples_2.11-2.3.0.jar spark-2.3.0-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.3.0.jar
    cp mllib/target/spark-mllib_2.11-2.3.0.jar spark-2.3.0-bin-hadoop2.7/jars/

## Example Programs

We provide sample programs in the `examples` directory.
To run MA-SGD example,

    $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.mllib.SVMWithMASGDExample \
    --master yarn \
    --num-executors $workerSize \
    $SPARK_HOME/examples/jars/spark-examples*.jar \
    $appName \
    $inputData \
    $stepSize \
    $regParam \
    $miniBatchFraction \
    $numIterations \
    $workerSize

will run the MA-SGD distributed example.

To run MA-LDA example,

    $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.ml.JavaMALDAExample \
    --master yarn \
    --num-executors $workerSize \
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

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

