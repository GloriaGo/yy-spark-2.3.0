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
cp $log_file $app_dir/YY-spark-bach101.log
touch $log_file
chmod 666 $log_file

for slave in $(cat slave_list)
do
	echo $slave
        scp $slave:/root/YY-logs/YY-spark.log $app_dir/YY-spark-${slave}.log
	cat $app_dir/YY-spark-${slave}.log | grep YY
done


