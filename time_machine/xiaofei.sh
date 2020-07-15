# !/bin/bach
#echo "开始安装"

#echo "Step0: 升级apt-get的包资源目录"
#apt-get update

#echo "Step1: 关闭防火墙"
#ufw disable

#echo "Step2: 安装java"
#apt-get install openjdk-8-jdk
#export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
#echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
#source ~/.bashrc


#echo "Step3: 下载配置Hadoop-2.7.7"
#wget https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz
#tar -zxvf hadoop-2.7.7.tar.gz
#export HADOOP_HOME=/root/hadoop-2.7.7
#export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#echo "export HADOOP_HOME=/root/hadoop-2.7.7" >> ~/.bashrc
#echo "export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop" >> ~/.bashrc
#echo "export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH" >> ~/.bashrc
#cp basic_files/yarn-site.xml $HADOOP_HOME/etc/hadoop/
#cp basic_files/core-site.xml $HADOOP_HOME/etc/hadoop/
#cp basic_files/hdfs-site.xml $HADOOP_HOME/etc/hadoop/
#cp basic_files/mapred-site.xml $HADOOP_HOME/etc/hadoop/
#cp basic_files/slaves $HADOOP_HOME/etc/hadoop/
#cp basic_files/hadoop-env.sh $HADOOP_HOME/etc/hadoop/

#echo "Step4: 下载配置Spark-2.4.6"
#wget https://mirrors.bfsu.edu.cn/apache/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz
#tar -zxvf spark-2.4.6-bin-hadoop2.7.tgz
#export SPARK_HOME=/root/spark-2.4.6-bin-hadoop2.7
#export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
#echo "export SPARK_HOME=/root/spark-2.4.6-bin-hadoop2.7" >> ~/.bashrc
#echo "export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH" >> ~/.bashrc
#cp basic_spark_files/spark-env.sh $SPARK_HOME/conf/
#cp basic_spark_files/spark-defaults.conf $SPARK_HOME/conf/
#cp basic_spark_files/log4j.properties $SPARK_HOME/conf/

#echo "Step5: 初始化HDFS"
#hdfs namenode -format
#start-dfs.sh
#hdfs dfs -mkdir -p hdfs://bach001:9000/user/gyy/
#hdfs dfs -put ~/xiaofei.sh hdfs://bach001:9000/user/gyy/
#hdfs dfs -ls hdfs://bach001:9000/user/gyy/

#echo "Step6: 测试Spark on Yarn"
#start-yarn.sh
#jps
#~/.yarn-test.sh

#echo "Step7: 下载配置Angel-3.1.0"
#wget https://github.com/Angel-ML/angel/archive/angel-3.1.0-bin.zip
#unzip angel-3.1.0-bin.zip
#export ANGEL_HOME=/root/angel-3.1.0-bin
#export ANGEL_HDFS_HOME=hdfs://bach001:9000/user/gyy/angel-3.1.0-bin
#echo "export ANGEL_HOME=/root/angel-3.1.0-bin" >> ~/.bashrc
#echo "export ANGEL_HDFS_HOME=hdfs://bach001:9000/user/gyy/angel-3.1.0-bin" >> ~/.bashrc
#hdfs dfs -put /root/angel-3.1.0-bin hdfs://bach001:9000/user/gyy/

echo "记得自己source ~/.bashrc"
