package com.yarenty.ddi.weather


import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import com.yarenty.ddi.MLProcessor
import com.yarenty.ddi.schemas.{OutputLine, SMOutputCSVParser}
import hex.Distribution
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.drf.{DRF, DRFModel}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{h2o, SparkContext, SparkFiles}
import water.Key
import water.fvec.{Vec, Frame}
import water.support.SparkContextSupport


import MLProcessor.h2oContext._
import MLProcessor.h2oContext.implicits._
import MLProcessor.sqlContext.implicits._
/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object WeatherPredic extends SparkContextSupport {


  val test_imp_dir = "/opt/data/season_1/outdata/w_"
  val train_imp_dir = "/opt/data/season_1/outdata/w_"


  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext





    println(s"\n\n LETS MODEL\n")
    val predictset = Array("2016-01-22_predict", "2016-01-26_predict", "2016-01-30_predict", "2016-01-28_predict", "2016-01-24_predict")

    val testset = Array("2016-01-22_test", "2016-01-26_test", "2016-01-30_test", "2016-01-28_test", "2016-01-24_test")
    for (p <- testset) addFiles(sc, absPath(test_imp_dir + p))
    for (p <- predictset) addFiles(sc, absPath(test_imp_dir + p))
    for (i <- 1 to 21) addFiles(sc, absPath(train_imp_dir + "2016-01-" + "%02d".format(i)))

    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("w_" + a))).toSeq
    val trainURIs = (1 to 21).map(a => new URI("file:///" + SparkFiles.get("w_2016-01-" + "%02d".format(a)))).toSeq

    // Use super-fast advanced H2O CSV parser !!!
    val smOutputTrain = new h2o.H2OFrame(WPCSVParser.get, trainURIs: _*)
    val smOutputTest = new h2o.H2OFrame(WPCSVParser.get, testURIs: _*)


    println(s"\n===> TRAIN: ${smOutputTrain.numRows()}\n")
    println(s"\n===> TEST: ${smOutputTest.numRows()}\n")


    val weatherModel = gbmModel(smOutputTrain, smOutputTest)
    val tempModel = gbmModel(smOutputTrain, smOutputTest)
    val pollutionModel = gbmModel(smOutputTrain, smOutputTest)

    // SAVE THE MODEL!!!
    var om = new FileOutputStream("/opt/data/DRFWeatherPredictModel" + System.currentTimeMillis() + ".java")
    weatherModel.toJava(om, false, false)


    for (pu <- predictset) {
      val smOutputPredict = new h2o.H2OFrame(WPCSVParser.get, new URI("file:///" + SparkFiles.get("w_" + pu)))
//      val predictTrainDemand = weatherModel.score(smOutputTest)
//
//      var vec = predictTrainDemand.get.lastVec
//
//      smOutputPredict.add("temp", vec)
//
//      saveOutput(smOutputPredict)


    }
    println("=========> off to go!!!")


  }


  def saveOutput(smOutputTest: H2OFrame): Unit = {

    import MLProcessor.sqlContext
    val names = Array("timeslice", "districtID", "gap", "predict")

    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val out = new Frame(key, names, smOutputTest.vecs(names))

    val zz = new h2o.H2OFrame(out)


    val odf = asDataFrame(zz)
    val o = odf.groupBy("timeslice", "districtID").agg(Map(
      "gap" -> "sum",
      "predict" -> "sum"
    ))
    o.rename("sum(gap)", "gap")
    o.rename("sum(predict)", "predict")

    o.take(20).foreach(println)

    val outTab = asRDD[OutputLine](o).collect()

    //calculate error
    var sum = 0.0d
    outTab.foreach(x => {
      sum += (x.predict.get - x.gap.get.toDouble).abs
    })

    val error = sum / outTab.length

    println("\nFINAL ERROR:" + error)

    //      val p = u.getPath.split("/")
    //      val n = p(p.length - 1)

    val csv = o.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/all_out2.csv"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close
  }


  def gbmModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): GBMModel = {

    val params = new GBMParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._ntrees = 100
    params._response_column = "gap"
    params._ignored_columns = Array("id")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val gbm = new GBM(params)

    println("GBM:" + gbm)

    gbm.trainModel.get

  }




}

