app_name=$1
mkdir $app_name
scp -r slave02:~/Documents/hadoop-slave02/logs/userlogs/$app_name/* $app_name/.
scp -r slave03:~/Documents/hadoop-slave03/logs/userlogs/$app_name/* $app_name/.
