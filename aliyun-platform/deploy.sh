#set openjdk-8-jdk env on driver ~/.bashrc file
apt-get update
apt-get install openjdk-8-jdk
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
echo "export JRE_HOME=$JAVA_HOME/jre" >> ~/.bashrc
echo "export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH" >> ~/.bashrc
echo "export CLASSPATH=.:$JAVA_HOME/lib" >> ~/.bashrc

#download pre-build official release of hadoop-2.7.7 and spark-2.3.0-bin-hadoop2.7 on driver
wget https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz
tar -zxvf hadoop-2.7.7.tar.gz
wget https://mirrors.bfsu.edu.cn/apache/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
tar -zxvf spark-2.3.0-bin-hadoop2.7.tgz
#set env of hadoop and spark
echo "export HADOOP_HOME=/root/hadoop-2.7.7" >> ~/.bashrc
echo "export HADOOP_CONF_DIR=/root/hadoop-2.7.7/etc/hadoop" >> ~/.bashrc
echo "export SPARK_HOME=/root/spark-2.3.0-bin-hadoop2.7" >> ~/.bashrc
echo "export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH" >> ~/.bashrc

#set configuration in hadoop-2.7.7/etc/hadoop and spark-2.3.0-bin-hadoop2.7/conf on driver

for slave in $(cat slave_list)
do
        #set ssh login without password between machines
        #ufw disable
        ssh $slave "apt-get update"
        ssh $slave "apt-get install openjdk-8-jdk"

        # after setting environment on driver
        scp ~/.bashrc $slave:~/.bashrc
        #login to machines to execute "source ~/.bashrc"

        # after setting haddop/etc on driver
        ssh $slave "rm -r ~/hadoop-2.7.7/"
        scp -r ~/hadoop-2.7.7 $slave:~/
        #scp -r ~/hadoop-2.7.7/etc/hadoop/ $slave:~/hadoop-2.7.7/etc/
done

# set HDFS on driver
ufw disable
hdfs namenode -format
start-dfs.sh
hdfs dfs -mkdir -p /user/root
