if [ $# != 2 ]
then
	echo "Usage: application_name and submit.sh are necessary parameters"
	exit 1
fi

app_name=$1
submit_file=$2
log_file="/tmp/YY-logs/YY-spark.log"
app_dir="/home/guoyunyan/spark-2.4.3-bin-hadoop2.6/logs/assemble-logs/$app_name"

mkdir $app_dir
cp $submit_file $app_dir/.
#cp /home/guoyunyan/spark-2.4.3-bin-hadoop2.6/logs/event-logs/$app_name $app_dir/.
# because in batch_submit, we don't know app_name in advance.
mkdir $app_dir/logs
cp $log_file $app_dir/logs/YY-spark-master.log
rm $log_file
touch $log_file
chmod 666 $log_file

for slave in $(cat ~/admin/slaves_list)
do
	echo $slave
        scp $slave:/tmp/YY-logs/YY-spark.log $app_dir/logs/YY-spark-${slave}.log
	ssh $slave "rm /tmp/YY-logs/YY-spark.log"
done
