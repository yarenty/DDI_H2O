package com.yarenty.ddi

import com.yarenty.ddi.DataMunging._
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
    //    val conf = new SparkConf()
    //      .setMaster("local-cluster[3,2,1024]")
    //      .setSparkHome("/opt/spark-1.6.1-bin-hadoop2.4")
    //      .setAppName("DDI Data Munging")
//
//    val conf = configure("H2O: DDI Data Munging")
//    val sc = new SparkContext(conf)
//
//    //val h2oContext = H2OContext.getOrCreate(sc)
//    val h2oContext = new H2OContext(sc).start()
//    import h2oContext._
//    import h2oContext.implicits._


    println(s"\n\n H2O CONTEXT is TOO !!!!!!\n")


    //do munging stuff
    DataMunging.process(h2oContext)

    // do modelling stuff
    BuildAdvancedModel.process(h2oContext)

    // Shutdown Spark cluster and H2O
    // h2oContext.stop(stopSparkContext = true)

  }

}
