workerNumber=$1
machineNumber=$1
coreNumber=1
seed=73

hdfs dfs -rm -r nyt_spark

../spark-2.4.3-bin-hadoop2.6/bin/spark-submit --master yarn \
	--class BalanceStorageJob \
	--conf spark.eventLog.enabled=true \
	--conf spark.locality.wait=0s \
	--conf spark.driver.memory=4g \
	--conf spark.driver.cores=1 \
	--conf spark.sql.files.maxPartitionBytes=536870912 \
	--conf spark.driver.maxResultSize=10g \
	--conf spark.rpc.message.maxSize=2000 \
	--conf spark.shuffle.file.buffer=64m \
	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	--conf spark.kryoserializer.buffer.max=2000m \
	--conf spark.memory.fraction=0.9 \
	--conf spark.task.cpus=1 \
	--conf spark.executor.heartbeatInterval=5s \
	--conf spark.executorextraJavaOptions=-XX:+UseG1GC \
	--num-executors $machineNumber \
	--executor-cores $coreNumber \
	--executor-memory 20g \
	../packages/RepartitionDataset.jar \
	repartition-nytimes \
	$workerNumber \
	hdfs://master.cluster:9000/user/guoyunyan/input/training_nytimes_libsvm.txt \
	hdfs://master.cluster:9000/user/guoyunyan/input/validate_nytimes_libsvm.txt \
	hdfs://master.cluster:9000/user/guoyunyan/input/test_nytimes_libsvm.txt \
	hdfs://master.cluster:9000/user/guoyunyan/nyt_spark/repar_nyt_training \
	hdfs://master.cluster:9000/user/guoyunyan/nyt_spark/repar_nyt_validate \
	hdfs://master.cluster:9000/user/guoyunyan/nyt_spark/repar_nyt_test
