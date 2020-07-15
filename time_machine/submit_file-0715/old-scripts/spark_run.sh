topic_size=100
sample_fraction=0.000127
optimizer="online"
tau0=1024
kappa=0.51
max_iter=1000
worker_num=8

submit_file="define_lda_submit_pubmed.sh"
#submit_file="define_lda_submit_nyt.sh"

# since the block_size of HDFS is 512M, when worker_size is small, multiply balance_size.
./pubmed_balance_dataset.sh 16
#./nyt_balance_dataset.sh $worker_num

./$submit_file $topic_size $sample_fraction $kappa $tau0 $max_iter $worker_num $optimizer
./assemble_logs.sh "test_pubmed_8w" /home/guoyunyan/scripts/spark_run.sh

