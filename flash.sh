./build/mvn -e -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.7 -Dmaven.test.skip=true clean package

cp examples/target/original-spark-examples_2.11-2.3.0.jar ../spark-2.3.0-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.3.0.jar
scp examples/target/original-spark-examples_2.11-2.3.0.jar bach001:~/

#./build/mvn -e -pl mllib clean package -Dmaven.test.skip=true -T 4
cp mllib/target/spark-mllib_2.11-2.3.0.jar ../spark-2.3.0-bin-hadoop2.7/jars/
scp mllib/target/spark-mllib_2.11-2.3.0.jar bach001:~/
