package com.yarenty.ddi

import java.io.{File, FileOutputStream, PrintWriter}
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
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkFiles, h2o}
import water.{AutoBuffer, Key}
import water.fvec.Frame
import water.support.SparkContextSupport

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object ShortNormModel extends SparkContextSupport {


  val data_dir = "/opt/data/season_1/outdatanorm/day_"


  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)


    println(s"\n\n LETS MODEL\n")


//    val testset = Array(22,24,26,28,30)
//    val trainset  = ((1 to 20)++testset).map(i => "2016-01-" + "%02d".format(i)).toArray

    //@TODO: test/present mode
    val testset = Array("2016-01-22_test")
    val trainset = Array("2016-01-01") ++ testset


    addFiles(sc, absPath(data_dir + "2016-01-21"))
    for (p <- trainset) addFiles(sc, absPath(data_dir + p))

    val trainURIs = trainset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq
    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("day_"+a))).toSeq
    // Use super-fast advanced H2O CSV parser !!!
    //last will be first ;-)
    var tmpTrain = new h2o.H2OFrame(SMOutputCSVParser.get, new URI("file:///" + SparkFiles.get("day_2016-01-21")))

    var df = asDataFrame(tmpTrain)

    println(s" SIZE: ${df.count} ")

    val toDel: Array[H2OFrame] = new Array[H2OFrame](trainset.length)
    //1 by 1 to avoid OOM!
    var i = 0
    for (tu <- trainURIs) {

      toDel(i) = new h2o.H2OFrame(SMOutputCSVParser.get, tu)

      df = df.unionAll(asDataFrame(toDel(i)))
      println(s" SIZE: ${df.count} ")
      i += 1
    }
    println(s" SIZE: ${df.count} ")


    val data = df.randomSplit(Array(0.8, 0.2), 1) //need to do it twice
    val trainData = asH2OFrame(data(0), "train")
    val testData = asH2OFrame(data(1), "test")


    trainData.colToEnum(Array("demand","timeslice", "districtID", "destDistrict", "weather"))
    testData.colToEnum(Array("demand","timeslice", "districtID", "destDistrict", "weather"))
    println(s" TRAIN/TEST DATA CREATED ");


    println(s"\n===> TRAIN: ${trainData.numRows()}\n")
    println(s"\n===>  TEST: ${testData.numRows()}\n")


    val gapModel = drfGapOnlyModel(trainData, testData)

    val omab = new FileOutputStream("/opt/data/DRFGapModel_" + System.currentTimeMillis() + ".hex_very_simple")
    val ab = new AutoBuffer (omab,true)
    gapModel.write(ab)
    ab.close()

    for (u <- testURIs) {


      val predictMe = new h2o.H2OFrame(SMOutputCSVParser.get, u)
      predictMe.colToEnum(Array("demand","timeslice", "districtID", "destDistrict", "weather"))

      val predict = gapModel.score(predictMe)
      val vec = predict.get.lastVec

      println("OUT VECTOR:" + vec.length)
      predictMe.add("predict", vec)
      saveOutput(predictMe, u.toString,sc)
    }
    println("=========> off to go!!!")
  }






  def saveOutput(smOutputTest: H2OFrame, fName: String, sc: SparkContext): Unit = {

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

//    val f = new RDD(sc,o)
    val toSee = new H2OFrame(o)

    println(s" output should be visible now ")

    val n = fName.split("/")
    val name = n(n.length - 1)

    val csv = o.toCSV(false, false)

    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/" + name + ".csv"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close

    println(s" CSV created: /opt/data/season_1/out/" + name + "_full.csv")

  }




  /******************************************************
    *
    * MODELS
    *
    *
    ******************************************************/


  def gbmModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): GBMModel = {

    val params = new GBMParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._ntrees = 100
    params._response_column = "gap"
    params._ignored_columns = Array("id","demand","weather")
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




  def drfGapOnlyModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key

    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather")
    params._ignore_const_cols = true
    params._ntrees = 20

    println("PARAMS:" + params.fullName)
    val drf = new DRF(params)


    println("DRF:" + drf)
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


  def dlDemandModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DeepLearningModel = {

    val params = new DeepLearningParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._distribution = Distribution.Family.gaussian
    params._response_column = "demand"
    params._ignored_columns = Array("id", "gap","weather")
    params._ignore_const_cols = true
    params._standardize = false

    //@TODO: removeme  (do bigger stuff)
    params._hidden = Array(40,40)
    params._mini_batch_size = 10
    params._epochs = 1.0

    println("PARAMS:" + params.fullName)
    val drf = new DeepLearning(params)

    println("DRF:" + drf)
    drf.trainModel.get

  }

}

