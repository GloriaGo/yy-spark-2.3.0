job_name=pubmed100

worker_num=$4
thread_num=6
tasks_per_worker=1
memory_size=10
topic_num=$2
epoch_num=$3

vocab_size=150000
alpha=0.01
beta=0.05

training_data=$1
# training_data="hdfs://master.cluster:9000/user/guoyunyan/angel_datasets/nytimes_angel/"

hdfs dfs -rm -r lda_data/model
hdfs dfs -rm -r lda_data/log

/home/guoyunyan/angel-2.1.0/bin/angel-submit \
        --angel.app.submit.class com.tencent.angel.ml.lda.LDARunner \
 	--angel.train.data.path $training_data \
 	--angel.log.path "hdfs://master.cluster:9000/user/guoyunyan/lda_data/log" \
	--angel.save.model.path "hdfs://master.cluster:9000/user/guoyunyan/lda_data/model" \
	--save.topic.word.distribution false \
	--save.doc.topic.distribution false \
	--save.word.topic false \
	--save.doc.topic false \
 	--action.type train \
 	--ml.epoch.num $epoch_num \
 	--ml.lda.word.num $vocab_size \
 	--ml.lda.topic.num $topic_num \
	--ml.lda.alpha $alpha \
	--ml.lda.beta $beta \
 	--angel.job.name $job_name \
 	--angel.am.memory.gb $memory_size \
	--angel.workergroup.number $worker_num \
 	--angel.worker.memory.gb $memory_size \
	--angel.worker.thread.num $thread_num \
	--angel.worker.task.number $tasks_per_worker \
	--angel.ps.number $worker_num \
 	--angel.ps.memory.gb $memory_size \
	--angel.task.data.storage.level memory 
