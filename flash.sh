#./build/mvn -e -pl examples clean package -Dmaven.test.skip=true -T 4

scp examples/target/original-spark-examples_2.11-2.3.0.jar bach001:~/

#./build/mvn -e -pl mllib clean package -Dmaven.test.skip=true -T 4

scp mllib/target/spark-mllib_2.11-2.3.0.jar bach001:~/
