package com.yarenty.ddi


import java.io.{PrintWriter, File}
import java.net.URI
import java.io.FileOutputStream

import com.yarenty.ddi.schemas.{OutputLine, SMOutputCSVParser}
import hex.Distribution
import hex.tree.drf.{DRFModel, DRF}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.gbm.{GBMModel, GBM}
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, h2o, SparkContext}
import org.apache.spark.h2o.{RDD, H2OFrame, DoubleHolder, H2OContext}

import water.Key
import water.fvec.Frame
import water.support.SparkContextSupport

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object BuildModel extends SparkContextSupport {


  val test_imp_dir = "/opt/data/season_1/outtrain/sm_"
  val train_imp_dir = "/opt/data/season_1/outtest/sm_"


  def process(sc: SparkContext, h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    println(s"\n\n LETS MODEL\n")

    val testset = Array("2016-01-22_test", "2016-01-26_test", "2016-01-30_test", "2016-01-28_test", "2016-01-24_test")
    for (p <- testset) addFiles(sc, absPath(test_imp_dir + p))
    for (i <- 1 to 21) addFiles(sc, absPath(train_imp_dir + "2016-01-" + "%02d".format(i)))

    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("sm_" + a))).toSeq
    val trainURIs = (1 to 21).map(a => new URI("file:///" + SparkFiles.get("sm_2016-01-" + "%02d".format(a)))).toSeq


    //    val trainURIs = testset.map(a => new URI("file:///" + SparkFiles.get("sm_" + a))).toSeq
    //    val testURIs = Array(new URI("file:///" + SparkFiles.get("sm_2016-01-01"))).toSeq

    // Use super-fast advanced H2O CSV parser !!!
    val smOutputTrain = new h2o.H2OFrame(SMOutputCSVParser.get, trainURIs: _*)
    val smOutputTest = new h2o.H2OFrame(SMOutputCSVParser.get, testURIs: _*)

    println(s"\n===> TRAIN: ${smOutputTrain.numRows()}\n")
    println(s"\n===> TEST: ${smOutputTest.numRows()}\n")


    val model = drfModel(smOutputTrain, smOutputTest)
    //    val model = gbmModel(smOutputTrain, smOutputTest)


    // SAVE THE MODEL!!!
    val om = new FileOutputStream("/opt/data/GRFModel_" + System.currentTimeMillis() + ".java")
    model.toJava(om, false, false)

    //clean before creating outputs
    smOutputTrain.delete()

    for (u <- testURIs) {

      val outFrame = new h2o.H2OFrame(SMOutputCSVParser.get, u)

      val predict = model.score(outFrame)

      //val predictionsFromModel = asRDD[DoubleHolder](predict).collect.map(_.result.getOrElse(Double.NaN))
      // println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))
      val vec = predict.get.lastVec

      println("OUT VECTOR:" + vec.length)

      outFrame.add("predict", vec)

      val inNames = Array("timeslice", "district ID", "gap", "predict")
      val outNames = Array("timeslice", "districtID", "gap", "predict")

      val key = Key.make("output").asInstanceOf[Key[Frame]]
      val out = new Frame(key, outNames, outFrame.vecs(inNames))

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



      val p = u.getPath.split("/")
      val n = p(p.length - 1)

      val csv = o.toCSV(true, false)
      val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/" + n + "_out2.csv"))
      while (csv.available() > 0) {
        csv_writer.write(csv.read.toChar)
      }
      csv_writer.close

      //outTab.delete()
      o.delete()
      outFrame.delete()
      predict.delete()

    }
    println("=========> off to go!!!")
    // get model


    //    predict model: "drf-64ae9c97-6435-4380-b0a9-65ef71569f08",
    //    frame: "sm_2016_01_01.hex",
    //    predictions_frame: "prediction-3ea340bc-36ac-4271-8062-9a1bc7b2ab60"


    //bindFrames "combined-prediction-3ea340bc-36ac-4271-8062-9a1bc7b2ab60",
    // [ "prediction-3ea340bc-36ac-4271-8062-9a1bc7b2ab60", "sm_2016_01_01.hex" ]


  }


  def gbmModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): GBMModel = {

    val params = new GBMParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._ntrees = 100
    params._response_column = "gap"
    params._ignored_columns = Array("id", "demand")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val gbm = new GBM(params)

    println("GBM:" + gbm)

    gbm.trainModel.get

  }


  //
  //    buildModel 'drf', {
  //      "model_id":"drf-64ae9c97-6435-4380-b0a9-65ef71569f08",
  //      "training_frame":"sm_2016_01_22_test.hex",
  //      "validation_frame":"sm_2016_01_01.hex",
  //      "nfolds":0,
  //      "response_column":"gap"
  //      ,"ignored_columns":["id"],"" +
  //        "ignore_const_cols":true,
  //      "ntrees":50,"max_depth":20,
  //      "min_rows":1,"nbins":20,"seed":-1,
  //      "mtries":-1,
  //      "sample_rate":0.6320000290870667,
  //      "score_each_iteration":false,
  //      "score_tree_interval":0,"nbins_top_level":1024,
  //      "nbins_cats":1024,"r2_stopping":0.999999,
  //      "stopping_rounds":0,"stopping_metric":"AUTO",
  //      "stopping_tolerance":0.001,"max_runtime_secs":0,
  //      "checkpoint":"","col_sample_rate_per_tree":1,
  //      "min_split_improvement":0,"histogram_type":"AUTO",
  //      "build_tree_one_node":false,"sample_rate_per_class":[],
  //      "binomial_double_trees":false,
  //      "col_sample_rate_change_per_level":1}


  def drfModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._distribution = Distribution.Family.poisson
    params._ntrees = 50
    params._response_column = "gap"
    params._ignored_columns = Array("id", "demand")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val drf = new DRF(params)


    println("DRF:" + drf)

    drf.trainModel.get

  }
}
