app_name=$1
mkdir $app_name
scp -r bach001:~/hadoop-2.7.7/logs/userlogs/$app_name $app_name/bach001-$app_name
scp -r bach002:~/hadoop-2.7.7/logs/userlogs/$app_name $app_name/bach002-$app_name
scp -r bach003:~/hadoop-2.7.7/logs/userlogs/$app_name $app_name/bach003-$app_name
scp -r bach004:~/hadoop-2.7.7/logs/userlogs/$app_name $app_name/bach004-$app_name
