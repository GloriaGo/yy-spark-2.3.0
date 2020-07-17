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
package org.apache.spark.mllib.clustering

import breeze.linalg.{max, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics._
import org.apache.commons.math3.util.FastMath

/**
 * Utility methods for LDA.
 */
private[clustering] object LDAUtils {
  /**
   * Log Sum Exp with overflow protection using the identity:
   * For any a: $\log \sum_{n=1}^N \exp\{x_n\} = a + \log \sum_{n=1}^N \exp\{x_n - a\}$
   */
  private[clustering] def logSumExp(x: BDV[Double]): Double = {
    val a = max(x)
    a + log(sum(exp(x -:- a)))
  }

  /**
   * For theta ~ Dir(alpha), computes E[log(theta)] given alpha. Currently the implementation
   * uses [[breeze.numerics.digamma]] which is accurate but expensive.
   */
  private[clustering] def dirichletExpectation(alpha: BDV[Double]): BDV[Double] = {
    digamma(alpha) - digamma(sum(alpha))
  }

  /**
   * Computes [[dirichletExpectation()]] row-wise, assuming each row of alpha are
   * Dirichlet parameters.
   */
  private[clustering] def dirichletExpectation(alpha: BDM[Double]): BDM[Double] = {
    val rowSum = sum(alpha(breeze.linalg.*, ::))
    val digAlpha = digamma(alpha)
    val digRowSum = digamma(rowSum)
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    result
  }

  // YY modify
  private[clustering] def digammaLowPrecision(x: Double, maxRecursive: Int): Double = {
    var v = 0.0
    if (x > 0 && x <= 1e-5) { // use method 5 from Bernardo AS103
      // accurate to O(x)
      v = -0.577215664901532 - 1 / x
    }
    else if (x >= maxRecursive) { // use method 4 (accurate to O(1/x^8)
      //            1       1        1         1
      // log(x) -  --- - ------ + ------- - -------
      //           2 x   12 x^2   120 x^4   252 x^6
      val inv = 1 / (x * x)
      v = FastMath.log(x) - 0.5 / x - inv * ((1.0 / 12) + inv * (1.0 / 120 - inv / 252))
    }
    else {
      val firstPart = x + maxRecursive - x.toInt
      val inv = 1 / (firstPart * firstPart)
      v = FastMath.log(firstPart) - 0.5 / firstPart -
        inv * ((1.0 / 12) + inv * (1.0 / 120 - inv / 252))
      var i = x
      while (i < maxRecursive) {
        v -= 1 / i
        i = i + 1
      }
    }
    return v
  }

  private[clustering] def dirExpLowPrecision(alpha: BDV[Double],
                                             maxRecursive: Int): BDV[Double] = {
    val digAlpha = alpha.map(x => digammaLowPrecision(x, maxRecursive))
    val digammaSum = digammaLowPrecision(sum(alpha), maxRecursive)
    digAlpha - digammaSum
  }

  private[clustering] def dirExpLowPrecision(alpha: BDM[Double], // k * v
                                             rowSum: BDV[Double],
                                             maxRecursive: Int): BDM[Double] = {
    val digAlpha = alpha.map(x => digammaLowPrecision(x, maxRecursive))
    val digRowSum = rowSum.map(x => digammaLowPrecision(x, maxRecursive))
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    result // k * v
  }

  private[clustering] def partDirExp(partAlpha: BDM[Double], rowSum: BDV[Double]): BDM[Double] = {
    val digAlpha = digamma(partAlpha)
    val digRowSum = digamma(rowSum)
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    result // k * ids
  }

  private[clustering] def dirichletExpectation(alpha: BDM[Double], ids: List[Int]): BDM[Double] = {
    val partAlpha = alpha.t(ids, ::).toDenseMatrix.t // k * ids
    val rowSum = sum(alpha(breeze.linalg.*, ::))
    partDirExp(partAlpha, rowSum)
  }

  private[clustering] def partDirExpLowPrecision(alpha: BDM[Double], ids: List[Int],
                                                 maxRecursive: Int): BDM[Double] = {
    val partAlpha = alpha.t(ids, ::).toDenseMatrix.t // k * ids
    val rowSum = sum(alpha(breeze.linalg.*, ::))
    val result = dirExpLowPrecision(partAlpha, rowSum, maxRecursive)
    result // k * ids
  }

  private[clustering] def normalize(alpha: BDM[Double], rowSum: BDV[Double]): BDM[Double] = {
    val result = alpha(::, breeze.linalg.*) / rowSum
    result
  }

}
