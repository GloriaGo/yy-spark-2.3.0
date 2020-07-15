if [ $# != 2 ]
then
	echo "Usage: application_name and submit.sh are necessary parameters"
	exit 1
fi

app_name=$1
submit_file=$2
log_file="/root/YY-logs/YY-spark.log"
app_dir="/root/YY-logs/assemble-logs/$app_name"

mkdir $app_dir
cp $submit_file $app_dir/.
#cp /home/guoyunyan/spark-2.4.3-bin-hadoop2.6/logs/event-logs/$app_name $app_dir/.
# because in batch_submit, we don't know app_name in advance.
mkdir $app_dir/logs
cp $log_file $app_dir/logs/YY-spark-bach001.log
rm $log_file
touch $log_file
chmod 666 $log_file

for slave in $(cat slaves_list)
do
	echo $slave
        scp $slave:/root/YY-logs/YY-spark.log $app_dir/logs/YY-spark-${slave}.log
	ssh $slave "rm /root/YY-logs/YY-spark.log"
	cat $app_dir/logs/YY-spark-${slave}.log | grep YYlog
done
