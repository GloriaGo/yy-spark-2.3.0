hdfs dfs -rm -r hdfs://bach001:9000/user/root/angel/input_test/model
hdfs dfs -rm -r hdfs://bach001:9000/user/root/angel/input_test/log

bin/angel-submit \
         --angel.app.submit.class com.tencent.angel.ml.core.graphsubmit.GraphRunner \
 	--angel.train.data.path "hdfs://bach001:9000/user/root/angel/input_test/a9a_123d_train.libsvm" \
 	--angel.log.path "hdfs://bach001:9000/user/root/angel/input_test/log" \
 	--angel.save.model.path "hdfs://bach001:9000/user/root/angel/input_test/model" \
 	--action.type train \
 	--ml.model.class.name com.tencent.angel.ml.classification.LogisticRegression \
 	--ml.epoch.num 10 \
 	--ml.data.type libsvm \
 	--ml.feature.index.range 1024 \
 	--angel.job.name LR_test \
	--angel.am.cpu.vcores 1 \
 	--angel.am.memory.gb 3 \
	--angel.worker.cpu.vcores 1 \
 	--angel.worker.memory.gb 3 \
	--angel.ps.cpu.vcores 1 \
 	--angel.ps.memory.gb 3 \
	--angel.ps.number 3 \
	--angel.workergroup.number 3

