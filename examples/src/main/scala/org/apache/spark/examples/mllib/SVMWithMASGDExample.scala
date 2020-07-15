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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.classification.GhandSVMSGDShuffleModel
import org.apache.spark.mllib.util.MLUtils
// $example off$

object SVMWithMASGDExample extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(args.apply(0))
    val sc = new SparkContext(conf)
    // Load training data that has been balanced stored.
    val training = MLUtils.loadLibSVMFile(sc, args.apply(1))
    // Run training algorithm to build the model
    val stepSize = args.apply(2).toDouble // 1.0
    val regParam = args.apply(3).toDouble // 0.01
    val miniBatchFraction = args.apply(4).toDouble // 1.0
    val numIterations = args.apply(5).toInt // 10

    val ghandmodel = GhandSVMSGDShuffleModel
    val startTime = System.nanoTime()
    val model = ghandmodel.train(training, numIterations, stepSize, regParam, miniBatchFraction)
    logInfo(s"YYlog=running_time_per_iter(s):${((System.nanoTime()-startTime)/1e9)/numIterations}")

    // Save and load model
    model.save(sc, "target/tmp/scalaSVMWithMASGDModel")
//    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithMASGDModel")
    // $example off$

    logInfo(s"YYlog=data:${args.apply(0)}")
    logInfo(s"YYlog=stepSize:${stepSize}=regParam:${regParam}")
    logInfo(s"YYlog=miniBatchFraction:${miniBatchFraction}=numIterations:${numIterations}")
    sc.stop()
  }
}
// scalastyle:on println
