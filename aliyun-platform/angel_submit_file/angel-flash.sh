cp ~/tmp/spark-on-angel-examples-3.1.0.jar $ANGEL_HOME/lib/
hdfs dfs -rm angel-3.1.0-bin/lib/spark-on-angel-examples-3.1.0.jar
hdfs dfs -put $ANGEL_HOME/lib/spark-on-angel-examples-3.1.0.jar angel-3.1.0-bin/lib/.

cp ~/tmp/spark-on-angel-examples-3.1.0-ps.jar $ANGEL_HOME/lib/
hdfs dfs -rm angel-3.1.0-bin/lib/spark-on-angel-examples-3.1.0-ps.jar
hdfs dfs -put $ANGEL_HOME/lib/spark-on-angel-examples-3.1.0-ps.jar angel-3.1.0-bin/lib/.
