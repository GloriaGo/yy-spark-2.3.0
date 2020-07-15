optimizer=$7
topic_size=$1
sampling_rate=$2
learning_decay=$3
learning_offset=$4
max_iter=$5
random_seed=73

workerNumber=$6
machineNumber=$6
coreNumber=1

../spark-2.4.3-bin-hadoop2.6/bin/spark-submit --master yarn \
	--class OnlineLDAJob \
	--conf spark.eventLog.enabled=true \
	--conf spark.locality.wait=0s \
	--conf spark.driver.memory=20g \
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
	../packages/SubmitOnlineLDA.jar \
	test_status_pubmed \
	hdfs://master.cluster:9000/user/guoyunyan/pubmed_spark/repar_pubmed_training \
	hdfs://master.cluster:9000/user/guoyunyan/pubmed_spark/repar_pubmed_validate \
	hdfs://master.cluster:9000/user/guoyunyan/pubmed_spark/repar_pubmed_test \
	$optimizer \
	$topic_size \
	$sampling_rate \
	$learning_decay \
	$learning_offset \
	$max_iter \
	$random_seed
