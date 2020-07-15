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
import org.apache.spark.sql.DataFrameWriter;
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
public class JavaRepartitionDataset {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("repartition")
                .getOrCreate();
        // Repartition
        Dataset<Row> datasetA = spark.read().format("libsvm").load(args[0]).repartition(Integer.parseInt(args[2])).cache();
        DataFrameWriter<Row> data1 = datasetA.write().format("libsvm");
        data1.save(args[1]);
        datasetA.unpersist();

        spark.stop();
    }
}
