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
public class JavaMALDAExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(args[0])
                .getOrCreate();
// Loads data.
        Dataset<Row> trainingData = spark.read().format("libsvm")
                .load(args[1]).cache();
        Dataset<Row> testData = spark.read().format("libsvm")
                .load(args[2]).cache();

// Trains a LDA model.
        LDA lda = new LDA().setOptimizer("ma")
                .setMaxIter(Integer.parseInt(args[3]))     // iteration
                .setK(Integer.parseInt(args[4]))   // topic number K
                .setSubsamplingRate(Double.parseDouble(args[5]))   // mini batch size fraction
                .setLearningDecay(Double.parseDouble(args[6]))   // kappa
                .setLearningOffset(Integer.parseInt(args[7]))   // tau0
                .setWorkerNumber(Integer.parseInt(args[8]))  // executor numbers
                .setSeed(11L);  // validate data generate based on this seed

        LDAModel model = lda.fitTest(trainingData, testData);

        double ll = model.logLikelihood(trainingData);
        double lp = model.logPerplexity(testData);
        System.out.println("YYlog=The lower bound on the log likelihood of the training dataset: " + ll);
        System.out.println("YYlog=The upper bound on perplexity of the test dataset: " + lp);

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
