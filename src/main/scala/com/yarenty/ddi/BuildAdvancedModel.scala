package com.yarenty.ddi

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import com.yarenty.ddi.schemas.{OutputLine, SMOutputCSVParser}
import hex.Distribution
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.drf.{DRF, DRFModel}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
import org.apache.commons.io.FileUtils
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{h2o, SparkContext, SparkFiles}
import water.{AutoBuffer, Key}
import water.fvec.{Vec, Frame}
import water.parser.{ParseSetup, ParseDataset}
import water.support.SparkContextSupport


import MLProcessor.h2oContext._
import MLProcessor.h2oContext.implicits._
import MLProcessor.sqlContext.implicits._

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object BuildAdvancedModel extends SparkContextSupport {


  val data_dir = "/opt/data/season_1/outdatanorm/day_"


  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    println(s"\n\n LETS MODEL\n")


    val testset = Array(22, 24, 26, 28, 30).map(i => "2016-01-" + "%02d".format(i) + "_test").toArray
    for (p <- testset) addFiles(sc, absPath(data_dir + p))
    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq
    //    val tmpTest = new h2o.H2OFrame(SMOutputCSVParser.get, testURIs(0))
    //    var dfTest = asDataFrame(tmpTest)

    //    1 by 1 to avoid OOM!
    //    for (tu <- testURIs.drop(1)) {
    //      val tmp = new h2o.H2OFrame(SMOutputCSVParser.get, tu)
    //      dfTest = dfTest.unionAll(asDataFrame(tmp))
    //      println(s" SIZE: ${dfTest.count} ")
    //    }
    //    val testData = asH2OFrame(dfTest,"test")

    //@TODO: 1 to 21
    val trainset = (14 to 21).map(i => "2016-01-" + "%02d".format(i)).toArray
    for (p <- trainset) addFiles(sc, absPath(data_dir + p))
    val trainURIs = trainset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq
    val tmpTrain = new h2o.H2OFrame(SMOutputCSVParser.get, trainURIs(0))
    var dfTrain = asDataFrame(tmpTrain)

    //1 by 1 to avoid OOM!
    for (tu <- trainURIs.drop(1)) {
      val tmp = new h2o.H2OFrame(SMOutputCSVParser.get, tu)
      dfTrain = dfTrain.unionAll(asDataFrame(tmp))
      println(s" SIZE: ${dfTrain.count} ")
    }
    //    val trainData = asH2OFrame(dfTrain,"train")

    val data = dfTrain.randomSplit(Array(0.8, 0.2), 1) //better results!
    val trainData = asH2OFrame(data(0), "train")
    val testData = asH2OFrame(data(1), "test")

    trainData.colToEnum(Array("day", "timeslice", "districtID", "destDistrict", "weather"))
    testData.colToEnum(Array("day", "timeslice", "districtID", "destDistrict", "weather"))

    println(s"\n===> TRAIN: ${trainData.numRows()}\n")
    println(s"\n===>  TEST: ${testData.numRows()}\n")


    val gapModel = drfGapModel(trainData, testData)
    //    val gapModel = dlGapModel(trainData, testData)
    // SAVE THE MODEL!!!
    val om = new FileOutputStream("/opt/data/DRFGapModel_" + System.currentTimeMillis() + ".java")
    gapModel.toJava(om, false, false)
    val omab = new FileOutputStream("/opt/data/DRFGapModel_" + System.currentTimeMillis() + ".hex")
    val ab = new AutoBuffer(omab, true)
    gapModel.write(ab)
    ab.close()
    println("JAVA and hex(iced) models saved.")

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

    //filtering
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


    val csvfull = odf.toCSV(true, false)
    val csvfull_writer = new PrintWriter(new File("/opt/data/season_1/out/final_" + name + "_full.csv"))
    while (csvfull.available() > 0) {
      csvfull_writer.write(csvfull.read.toChar)
    }
    csvfull_writer.close

    println(s" CSV created: /opt/data/season_1/out/final_" + name + ".csv")

  }


  /** ****************************************************
    *
    * MODELS
    *
    *
    * *****************************************************/


  def gbmModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): GBMModel = {

    val params = new GBMParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._ntrees = 100
    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather", "temp")
    params._ignore_const_cols = true

    println("BUILDING:" + params)
    val gbm = new GBM(params)
    gbm.trainModel.get
  }


  //    buildModel 'drf', {
  //      "model_id":"drf-64ae9c97-6435-4380-b0a9-65ef71569f08",
  //      "training_frame":"sm_2016_01_22_test.hex",
  //      "validation_frame":"sm_2016_01_01.hex",
  //      "nfolds":0,
  //      "response_column":"gap"
  //      "ignored_columns":["id","demand"],
  //      "ignore_const_cols":true,
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


  def drfGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key

    params._ntrees = 20 //@todo use more
    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather", "temp")
    params._ignore_const_cols = false
    //    params._nbins = 50
    //    params._max_depth = 10

    println("BUILDING:" + params.fullName)
    val drf = new DRF(params)
    drf.trainModel.get
  }


  // here version of DL which works - no regularization as is already provided
  // and batch set to 10
  //
  //buildModel 'deeplearning', {"model_id":"deeplearning-9c73716a-1790-4fa1-a116-79ffa8f93133",
  // "training_frame":"train","validation_frame":"day_2016_01_22_test.hex",
  // "nfolds":0,"response_column":"gap",
  // "ignored_columns":["id","demand"],
  // "ignore_const_cols":true,"activation":"Rectifier",
  // "hidden":[200,200],"epochs":10,"
  // variable_importances":false,"score_each_iteration":false,"checkpoint":"",
  // "use_all_factor_levels":true,"standardize":false,
  // "train_samples_per_iteration":-2,"adaptive_rate":true,"input_dropout_ratio":0,"l1":0,"l2":0,"loss":"Automatic","distribution":"gaussian","score_interval":5,"score_training_samples":10000,"score_validation_samples":0,"score_duty_cycle":0.1,"stopping_rounds":5,"stopping_metric":"AUTO","stopping_tolerance":0,"max_runtime_secs":0,"autoencoder":false,"pretrained_autoencoder":"","overwrite_with_best_model":true,"target_ratio_comm_to_comp":0.05,"seed":4474453303064774000,"rho":0.99,"epsilon":1e-8,"max_w2":"Infinity","initial_weight_distribution":"UniformAdaptive","regression_stop":0.000001,"diagnostics":true,"fast_mode":true,"force_load_balance":true,"single_node_mode":false,"shuffle_training_data":false,"missing_values_handling":"MeanImputation","quiet_mode":false,"sparse":false,"col_major":false,"average_activation":0,"sparsity_beta":0,"max_categorical_features":2147483647,"reproducible":false,"export_weights_and_biases":false,
  // "mini_batch_size":"10","elastic_averaging":false}


  def dlGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DeepLearningModel = {

    val params = new DeepLearningParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._distribution = Distribution.Family.gaussian
    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather")
    params._ignore_const_cols = true
    //params._standardize = false

    //@TODO: removeme  (do bigger stuff)
    //    params._hidden = Array(50,50)
    params._mini_batch_size = 10
    params._epochs = 5.0

    println("BUILDING:" + params.fullName)
    val drf = new DeepLearning(params)
    println("DRF:" + drf)
    drf.trainModel.get
  }

}

