package com.yarenty.ddi.math

import org.apache.commons.math.MathException
import org.apache.commons.math.distribution.FDistribution
import org.apache.commons.math.distribution.FDistributionImpl
import org.apache.commons.math.stat.StatUtils
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression

/**
  * Based on Sergey Edunov java solution.
  * Created by yarenty on 27/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class GrangerTestResult(fStat: Double, r3: Double, pValue: Double) {
}

class Granger {

  /**
    * Returns p-value for Granger causality test.
    *
    * @param y - predictable variable
    * @param x - predictor
    * @param L - lag, should be 1 or greater.
    * @return p-value of Granger causality
    */
  def granger(y: Array[Double], x: Array[Double], L: Int): GrangerTestResult = {
    val h0 = new OLSMultipleLinearRegression()
    val h1 = new OLSMultipleLinearRegression()

    val laggedY = createLaggedSide(L, Array(y))

    val laggedXY = createLaggedSide(L, Array(x, y))

    val n = laggedY.length

    h0.newSampleData(strip(L, y), laggedY)
    h1.newSampleData(strip(L, y), laggedXY)

    val rs0 = h0.estimateResiduals()
    val rs1 = h1.estimateResiduals()

    val TSS1 = tss(strip(L, y))

    val RSS0 = sqrSum(rs0)
    val RSS1 = sqrSum(rs1)

    val ftest = ((RSS0 - RSS1) / L) / (RSS1 / (n - 2 * L - 1))

    val fDist = new FDistributionImpl(L, n - 2 * L - 1)
    try {
      val pValue = 1.0 - fDist.cumulativeProbability(ftest)
      return new GrangerTestResult(ftest, (1 - RSS1 / TSS1), pValue)
    } catch {
      case e: MathException => throw new RuntimeException(e)
    }
  }

  def tss(y: Array[Double]): Double = {
    var res = 0.0
    val avg = StatUtils.mean(y)
    for (yi <- y) {
      res += (yi - avg) * (yi - avg);
    }
    res
  }


  def createLaggedSide(L: Int, a: Array[Array[Double]]): Array[Array[Double]] = {
    val n = a(0).length - L
    val res = Array.ofDim[Double](n, L * a.length + 1)

    for (i <- 0 to a.length) {
      val ai = a(i)
      for (l <- 0 to L) {
        for (j <- 0 to n) {
          res(j)(i * L + l) = ai(l + j)
        }
      }
    }

    for (i <- 0 to n) {
      res(i)(L * a.length) = 1
    }
    res
  }

  def sqrSum(a: Array[Double]): Double = {
    var res = 0.0
    for (v <- a) {
      res += v * v
    }
    res
  }

  def strip(l: Int, a: Array[Double]): Array[Double] = {
    val res = Array[Double](a.length - l)
    System.arraycopy(a, l, res, 0, res.length);
    res
  }

}
