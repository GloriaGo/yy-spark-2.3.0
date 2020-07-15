#!/bin/bash

# Before run Spark on Angel application, you must follow the steps:
# 1. confirm Hadoop and Spark have ready in your environment
# 2. unzip angel-<version>-bin.zip to local directory
# 3. upload angel-<version>-bin directory to HDFS
# 4. set the following variables, SPARK_HOME, ANGEL_HOME, ANGEL_HDFS_HOME, ANGEL_VERSION
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_HOME=/root/spark-2.4.6-bin-hadoop2.7
export ANGEL_HOME=/root/angel-3.1.0-bin
export ANGEL_HDFS_HOME=hdfs://bach001:9000/user/root/angel-3.1.0-bin
export ANGEL_VERSION=3.1.0



scala_jar=scala-library-2.11.8.jar
angel_ps_external_jar=fastutil-7.1.0.jar,htrace-core-2.05.jar,sizeof-0.3.0.jar,kryo-shaded-4.0.0.jar,minlog-1.3.0.jar,memory-0.8.1.jar,commons-pool-1.6.jar,netty-all-4.1.18.Final.jar,hll-1.6.0.jar,stream-2.7.0.jar
angel_ps_jar=angel-ps-graph-${ANGEL_VERSION}.jar,angel-ps-core-${ANGEL_VERSION}.jar,angel-ps-psf-${ANGEL_VERSION}.jar,angel-ps-mllib-${ANGEL_VERSION}.jar,spark-on-angel-mllib-${ANGEL_VERSION}-ps.jar,spark-on-angel-graph-${ANGEL_VERSION}-ps.jar

sona_jar=spark-on-angel-core-${ANGEL_VERSION}.jar,spark-on-angel-mllib-${ANGEL_VERSION}.jar,spark-on-angel-graph-${ANGEL_VERSION}.jar
sona_external_jar=fastutil-7.1.0.jar,htrace-core-2.05.jar,sizeof-0.3.0.jar,kryo-shaded-4.0.0.jar,minlog-1.3.0.jar,memory-0.8.1.jar,commons-pool-1.6.jar,netty-all-4.1.18.Final.jar,hll-1.6.0.jar,json4s-jackson_2.11-3.2.11.jar,stream-2.7.0.jar,jniloader-1.1.jar,native_system-java-1.1.jar,arpack_combined_all-0.1.jar,core-1.1.2.jar,netlib-native_ref-linux-armhf-1.1-natives.jar,netlib-native_ref-linux-i686-1.1-natives.jar,netlib-native_ref-linux-x86_64-1.1-natives.jar,netlib-native_system-linux-armhf-1.1-natives.jar,netlib-native_system-linux-i686-1.1-natives.jar,netlib-native_system-linux-x86_64-1.1-natives.jar

dist_jar=${angel_ps_external_jar},${angel_ps_jar},${scala_jar},${sona_jar}
local_jar=${sona_external_jar},${angel_ps_jar},${sona_jar}


unset SONA_ANGEL_JARS
for f in `echo $dist_jar | awk -F, '{for(i=1; i<=NF; i++){ print $i}}'`; do
	jar=${ANGEL_HDFS_HOME}/lib/${f}
    if [ "$SONA_ANGEL_JARS" ]; then
        SONA_ANGEL_JARS=$SONA_ANGEL_JARS,$jar
    else
        SONA_ANGEL_JARS=$jar
    fi
done
echo SONA_ANGEL_JARS: $SONA_ANGEL_JARS
export SONA_ANGEL_JARS 


unset SONA_SPARK_JARS
for f in `echo $local_jar | awk -F, '{for(i=1; i<=NF; i++){ print $i}}'`; do
	jar=${ANGEL_HOME}/lib/${f}
    if [ "$SONA_SPARK_JARS" ]; then
        SONA_SPARK_JARS=$SONA_SPARK_JARS,$jar
    else
        SONA_SPARK_JARS=$jar
    fi
done
echo SONA_SPARK_JARS: $SONA_SPARK_JARS
export SONA_SPARK_JARS
