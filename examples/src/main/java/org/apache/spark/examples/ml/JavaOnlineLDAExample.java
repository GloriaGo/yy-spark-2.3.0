/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.ml;
// $example on$
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

/**
 * An example demonstrating LDA.
 * Run with
 * <pre>
 * bin/run-example ml.JavaLDAExample
 * </pre>
 */
public class JavaOnlineLDAExample {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .appName(args[0])
            .getOrCreate();
// Loads data.
    Dataset<Row> trainingData = spark.read().format("libsvm")
            .load(args[1]).cache();
    Dataset<Row> validateData = spark.read().format("libsvm")
            .load(args[2]).cache();
    Dataset<Row> testData = spark.read().format("libsvm")
            .load(args[3]).cache();

// Trains a LDA model.
    LDA lda = new LDA().setOptimizer(args[4])
            .setK(Integer.parseInt(args[5]))   // topic number K
            .setSubsamplingRate(Double.parseDouble(args[6]))   // mini batch size fraction
            .setLearningDecay(Double.parseDouble(args[7]))   // kappa
            .setLearningOffset(Integer.parseInt(args[8]))   // tau0
            .setMaxIter(Integer.parseInt(args[9]))     // iteration
            .setSeed(Long.parseLong(args[10]));  // validate data generate based on this seed

        LDAModel model = lda.fit(trainingData);
//    LDAModel model = lda.fitTest(trainingData, validateData);

        double ll = model.logLikelihood(trainingData);
        double lp = model.logPerplexity(testData);
        System.out.println("YY=The lower bound on the log likelihood of the training dataset: " + ll);
        System.out.println("YY=The upper bound on perplexity of the test dataset: " + lp);

//// Describe topics.
//        Dataset<Row> topics = model.describeTopics(3);
//        System.out.println("The topics described by their top-weighted terms:");
//        topics.show(false);
//
//// Shows the result.
//        Dataset<Row> transformed = model.transform(trainingData);
//        transformed.show(false);
        spark.stop();
  }
}
