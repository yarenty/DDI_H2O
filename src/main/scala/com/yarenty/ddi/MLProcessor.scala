package com.yarenty.ddi

import com.yarenty.ddi.DataMunging._
import com.yarenty.ddi.normalized.{NormalizedDataMungingTest, NormalizedDataMunging}
import com.yarenty.ddi.traffic.{TrafficPredic, TrafficPrediction}
import com.yarenty.ddi.weather.{WeatherPredic, WeatherPrediction}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.h2o.H2OContext
import water.support.SparkContextSupport

/**
  * Created by yarenty on 29/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object MLProcessor extends SparkContextSupport {

  val conf = configure("H2O: DDI Data Munging")
  val sc = new SparkContext(conf)

  //val h2oContext = H2OContext.getOrCreate(sc)
  val h2oContext = new H2OContext(sc).start()

  import h2oContext._
  import h2oContext.implicits._

  implicit val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]) {

    println(s"\n\n H2O CONTEXT is TOO !!!!!!\n")

//    WeatherPrediction.process(h2oContext)
//    WeatherPredic.process(h2oContext)
//
    //   TrafficPredictionTrain.process(h2oContext)
    //   TrafficPredictionTest.process(h2oContext)
    //   TrafficPrediction.process(h2oContext)
//    TrafficPredic.process(h2oContext)
//
//    DataMunging.process(h2oContext)
//    BuildModel.process(h2oContext)
//
//    NormalizedDataMunging.process(h2oContext)
//    NormalizedDataMungingTest.process(h2oContext)
    BuildAdvancedModel.process(h2oContext)
//    ShortNormModel.process(h2oContext)


    // Shutdown Spark cluster and H2O
    // h2oContext.stop(stopSparkContext = true)

  }

}
