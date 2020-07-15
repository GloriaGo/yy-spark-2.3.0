for slave in $(cat slaves_list)
do
	ssh $slave "mkdir ~/YY-logs"
	ssh $slave "chmod 777 ~/YY-logs"
	ssh $slave "touch ~/YY-logs/YY-spark.log"
	ssh $slave "chmod 666 ~/YY-logs/YY-spark.log"
done

