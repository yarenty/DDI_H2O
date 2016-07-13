package com.yarenty.ddi.utils

import java.io.{File, FileInputStream, PrintWriter}
import java.net.URI

import com.yarenty.ddi.MLProcessor._
import com.yarenty.ddi.MLProcessor.h2oContext._
import com.yarenty.ddi.schemas.OutputCSVParser
import org.apache.spark.h2o.{RDD, H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, h2o}
import water.MRTask
import water.fvec.Frame

import water.fvec.Chunk
import water.support.SparkContextSupport


/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object OutputFileMerger extends SparkContextSupport {


  val data_dir = "/opt/data/season_1/out/final_day_"

  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)
    println(s"Merge output files.")

    val outset = Array(22, 24, 26, 28, 30).map(i => "2016-01-" + "%02d".format(i) + "_test.csv").toArray
    for (p <- outset) addFiles(sc, absPath(data_dir + p))
    val outURIs = outset.map(a => new URI("file:///" + SparkFiles.get("final_day_" + a))).toSeq

    val tmpTrain = new h2o.H2OFrame(OutputCSVParser.get, outURIs(0))

    var dfTrain = asDataFrame(tmpTrain)
    dfTrain.registerTempTable("out")

    //filtering
    var a = sqlContext.sql("select concat('2016-01-22_', timeslice) as timeslice, districtID, gap, predict from out")

    for (tu <- outURIs.drop(1)) {
      val tmp = new h2o.H2OFrame(OutputCSVParser.get, tu)
      val tT = asDataFrame(tmp)
      tT.registerTempTable("b")

      val n = tu.toString.split("/")
      val name = n(n.length - 1).substring(10, 21)
      val b = sqlContext.sql("select concat('" + name + "',timeslice) as timeslice, districtID, gap, predict from b")
      a = a.unionAll(b)

    }

    saveOutput(asH2OFrame(a))

    println("FINAL FILE delivered")
  }

  def saveOutput(output: H2OFrame): Unit = {

    val csv = output.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/full.csv"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close

  }


}

