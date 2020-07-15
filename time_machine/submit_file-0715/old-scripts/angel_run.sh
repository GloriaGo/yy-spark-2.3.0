training_data="hdfs://master.cluster:9000/user/guoyunyan/angel_datasets/nips_angel/"
topic_size=50
epoch=10000
worker_size=4

screen_print_dir="/home/guoyunyan/angel-2.1.0/logs/"
log_name=$screen_print_dir"print.log"
rm $log_name

/home/guoyunyan/scripts/submit_angel_lda.sh $training_data $topic_size $epoch $worker_size>> $log_name 2>&1
