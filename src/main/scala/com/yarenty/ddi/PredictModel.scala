package com.yarenty.ddi

import java.io.{FileInputStream, File, FileOutputStream, PrintWriter}
import java.net.URI

import com.yarenty.ddi.MLProcessor.h2oContext._
import com.yarenty.ddi.MLProcessor.h2oContext.implicits._
import com.yarenty.ddi.schemas.SMOutputCSVParser
import hex.Distribution
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.drf.{DRF, DRFModel}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
import org.apache.commons.io.FileUtils
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, h2o}
import water.{AutoBuffer, Key}
import water.fvec.Frame
import water.support.SparkContextSupport

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object PredictModel extends SparkContextSupport {


  val data_dir = "/opt/data/season_1/outdatanorm/day_"


  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)


    println(s"\n\n LETS MODEL\n")


    val testset = Array(22, 24, 26, 28, 30).map(i => "2016-01-" + "%02d".format(i) + "_test").toArray
    for (p <- testset) addFiles(sc, absPath(data_dir + p))
    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq


    println("DRF Model")
    val gapModel = new DRFModel(null, null, null)
    println("loading...")

    val omab = new FileInputStream("/opt/data/season_1/out_263/DRFGapModel_1468242944125.hex")
    val ab = new AutoBuffer(omab)
    gapModel.read(ab)
    println("MODEL LOADED")
    ab.close()
    println("finally...")


    for (u <- testURIs) {
      val predictMe = new h2o.H2OFrame(SMOutputCSVParser.get, u)
      predictMe.colToEnum(Array("day", "timeslice", "districtID", "destDistrict", "weather"))

      val predict = gapModel.score(predictMe)
      val vec = predict.get.lastVec

      predictMe.add("predict", vec)
      println("OUT VECTOR:" + vec.length)
      saveOutput(predictMe, u.toString)
    }
    println("=========> off to go!!!")


  }


  def saveOutput(smOutputTest: H2OFrame, fName: String): Unit = {

    import MLProcessor.sqlContext
    val names = Array("timeslice", "districtID", "gap", "predict")

    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val out = new Frame(key, names, smOutputTest.vecs(names))

    val zz = new h2o.H2OFrame(out)

    val odf = asDataFrame(zz)

    odf.registerTempTable("out")

    val a = sqlContext.sql("select timeslice, districtID, gap, IF(predict<0.5,cast(0.0 as double), predict) as predict from out")
    a.registerTempTable("gaps")
    val o = sqlContext.sql(" select timeslice, districtID, sum(gap) as gap, sum(predict) as predict from gaps " +
      "  group by timeslice,districtID")


    o.take(20).foreach(println)

    val toSee = new H2OFrame(o)

    println(s" output should be visible now ")

    val n = fName.split("/")
    val name = n(n.length - 1)
    val csv = o.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/final_" + name + ".csv"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close
  }


}

