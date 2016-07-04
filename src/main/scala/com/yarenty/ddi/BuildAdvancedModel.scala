package com.yarenty.ddi

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

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
object BuildAdvancedModel extends SparkContextSupport {


  val test_imp_dir = "/opt/data/season_1/outdata/day_"
  val train_imp_dir = "/opt/data/season_1/outdata/day_"



  def process(h2oContext:H2OContext) {

    val sc = h2oContext.sparkContext





    println(s"\n\n LETS MODEL\n")

    val testset = Array("2016-01-22_test", "2016-01-26_test", "2016-01-30_test", "2016-01-28_test", "2016-01-24_test")
    for (p <- testset) addFiles(sc, absPath(test_imp_dir + p))
    for (i <- 1 to 21) addFiles(sc, absPath(train_imp_dir + "2016-01-" + "%02d".format(i)))

    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq
    val trainURIs = (1 to 21).map(a => new URI("file:///" + SparkFiles.get("day_2016-01-" + "%02d".format(a)))).toSeq

    // Use super-fast advanced H2O CSV parser !!!
    val smOutputTrain = new h2o.H2OFrame(SMOutputCSVParser.get, trainURIs: _*)
    val smOutputTest = new h2o.H2OFrame(SMOutputCSVParser.get, testURIs: _*)

    println(s"\n===> TRAIN: ${smOutputTrain.numRows()}\n")
    println(s"\n===> TEST: ${smOutputTest.numRows()}\n")


    /*
    val demandModel = drfDemandModel(smOutputTrain, smOutputTest)

    // SAVE THE MODEL!!!
    var om = new FileOutputStream("/opt/data/GRFDemandModel_" + System.currentTimeMillis() + ".java")
    demandModel.toJava(om, false, false)

    val predictTrainDemand = demandModel.score(smOutputTest)
    var vec = predictTrainDemand.get.lastVec
    smOutputTest.rename("demand","olddemand")
    smOutputTest.add("demand",vec)
    println("OUT VECTOR:" + vec.length)

    smOutputTrain.add("olddemand", Vec.makeZero(smOutputTrain.numRows()))

    saveDemandOutput(smOutputTest)


    val gapModel = drfGapModel(smOutputTrain, smOutputTest)
    // SAVE THE MODEL!!!
    om = new FileOutputStream("/opt/data/GRFGAPModel_" + System.currentTimeMillis() + ".java")
    gapModel.toJava(om, false, false)

    val predict = gapModel.score(smOutputTest)
    vec = predict.get.lastVec

    println("OUT VECTOR:" + vec.length)

    smOutputTest.add("predict", vec)

    saveOutput(smOutputTest)

    smOutputTest.rename("olddemand","demand")
*/

//      outFrame.delete()
//      predict.delete()
       // model.deviance()


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




  def saveDemandOutput(smOutputTest: H2OFrame): Unit = {


    import MLProcessor.sqlContext
    val names = Array("timeslice", "districtID", "demand", "olddemand")

    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val out = new Frame(key, names, smOutputTest.vecs(names))

    val zz = new h2o.H2OFrame(out)


    val odf = asDataFrame(zz)
    val o = odf.groupBy("timeslice", "districtID").agg(Map(
      "demand" -> "sum",
      "olddemand" -> "sum"
    ))
    o.rename("sum(demand)", "demand")
    o.rename("sum(olddemand)", "olddemand")

    o.take(20).foreach(println)


    val csv = o.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/demand_out1.csv"))
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


  def drfDemandModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    //params._distribution = Distribution.Family.AUTO

    params._ntrees = 20
    params._response_column = "demand"
    params._ignored_columns = Array("id","gap")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val drf = new DRF(params)


    println("DRF:" + drf)

    drf.trainModel.get

  }

  def drfGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
   // params._distribution = Distribution.Family.gaussian

    params._ntrees = 20
    params._response_column = "gap"
    params._ignored_columns = Array("id","olddemand")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val drf = new DRF(params)


    println("DRF:" + drf)

    drf.trainModel.get

  }
}
