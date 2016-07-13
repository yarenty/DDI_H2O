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



    println(">>>  PHASE :: LOADING TRAIN DATA")
    //@TODO: this is test/presentation mode
    //    val trainset  = ((1 to 20)++testset).map(i => "2016-01-" + "%02d".format(i)).toArray   //fullmode
    val trainset = Array("2016-01-21","2016-01-20")
    for (p <- trainset) addFiles(sc, absPath(data_dir + p))
    val trainURIs = trainset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq

    //1 by 1 to avoid OOM!
    val tmpTrain = new h2o.H2OFrame(SMOutputCSVParser.get, trainURIs(0))
    var df = asDataFrame(tmpTrain)
    println(s" SIZE: ${df.count} ")
    for (tu <- trainURIs.drop(1)) {
      val tmpDF = new h2o.H2OFrame(SMOutputCSVParser.get, tu)
      df = df.unionAll(asDataFrame(tmpDF))
      println(s" SIZE: ${df.count} ")
    }

    val trainData = asH2OFrame(df)
//    val data = df.randomSplit(Array(0.8, 0.2), 1)
//    val trainData = asH2OFrame(data(0), "train")
//    val testData = asH2OFrame(data(1), "test")



    println(">>>  PHASE :: LOADING TEST/VALIDATION DATA")
    //    val testset = Array(22,24,26,28,30)  //full mode
    val testset = Array("2016-01-22_test")
    for (p <- testset) addFiles(sc, absPath(data_dir + p))
    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("day_"+a))).toSeq
    val testData = new h2o.H2OFrame(SMOutputCSVParser.get,testURIs(0))

    trainData.colToEnum(Array("day","timeslice", "districtID", "destDistrict", "weather"))
    testData.colToEnum(Array("day","timeslice", "districtID", "destDistrict", "weather"))
    println(s"\n===> TRAIN: ${trainData.numRows()}\n")
    println(s"\n===>  TEST: ${testData.numRows()}\n")




    println(">>>  PHASE :: MODELING")
    val gapModel = drfGapModel(trainData, testData)


    println(">>>  PHASE :: SAVE MODEL FOR FUTURE USE")
    val omab = new FileOutputStream("/opt/data/DRFGapModel_" + System.currentTimeMillis() + ".hex_very_simple")
    val ab = new AutoBuffer(omab,true)
    gapModel.write(ab)
    ab.close()
    println("model saved!")




    println(">>>  PHASE :: PREDICTION")
    for (u <- testURIs) {
      val predictMe = new h2o.H2OFrame(SMOutputCSVParser.get, u)
      predictMe.colToEnum(Array("day","timeslice", "districtID", "destDistrict", "weather"))

      val predict = gapModel.score(predictMe)
      val vec = predict.get.lastVec
      predictMe.add("predict", vec)
      saveOutput(predictMe, u.toString,sc)
    }

    println(">>>  END")
    println("=========> off to go!!!")
  }




  /**
    * Saving output into CSV file
    * @param frame
    * @param fName
    * @param sc
    */
  def saveOutput(frame: H2OFrame, fName: String, sc: SparkContext): Unit = {

    import MLProcessor.sqlContext
    val names = Array("timeslice", "districtID", "gap", "predict")
    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val output = new Frame(key, names, frame.vecs(names))
    val odf = asDataFrame(new h2o.H2OFrame(output))

    odf.registerTempTable("out")
    val filter = sqlContext.sql("select timeslice, districtID, gap, IF(predict<0.5,cast(0.0 as double), predict) as predict from out")
    filter.registerTempTable("gaps")
    val predictions = sqlContext.sql(" select timeslice, districtID, sum(gap) as gap, sum(predict) as predict from gaps  group by timeslice, districtID")

    predictions.take(20).foreach(println)
    val toSee = new H2OFrame(predictions)
    println(s" output should be visible now ")

    val n = fName.split("/")
    val name = n(n.length - 1)
    val csv = predictions.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/short_" + name + ".csv"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close

    println(s" CSV created in: /opt/data/season_1/out/short_" + name + ".csv")

  }




  /******************************************************
    *
    * MODELS
    *
    ******************************************************/


  def gbmGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): GBMModel = {

    val params = new GBMParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._ntrees = 100
    params._response_column = "gap"
    params._ignored_columns = Array("id","day","weather","temp")
    params._ignore_const_cols = true
    params._score_each_iteration = true

    println("MODEL:" + params)
    val gbm = new GBM(params)
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




  def drfGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key

    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather", "temp","day")
    params._ignore_const_cols = true
    params._ntrees = 10
    params._max_depth = 20
    params._seed = -584220444440873026L
    params._score_each_iteration = true

    println("MODEL:" + params.fullName)
    val drf = new DRF(params)
    drf.trainModel.get
  }


  // here version of DL which works - no regularization as is already provided
  // and batch set to 10
  //
  //buildModel 'deeplearning', {"model_id":"deeplearning-9c73716a-1790-4fa1-a116-79ffa8f93133",
  // "training_frame":"train","validation_frame":"day_2016_01_22_test.hex",
  // "nfolds":0,"response_column":"gap",
  // "ignored_columns":["id","day","weather"],
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
    params._ignored_columns = Array("id", "weather", "temp", "day")  //day as too small
    params._ignore_const_cols = true
    params._standardize = true   //data is standardized
    params._score_each_iteration = true


//    params._hidden = Array(200,200) //Feel Lucky   - 3.17
//    params._hidden = Array(512) //Eagle Eye     -4.67
    params._hidden = Array(64,64,64) //Puppy Brain   -2.64
//    params._hidden = Array(32,32,32,32,32) //Junior Chess Master  (params._standardize = false)  2.88

    params._mini_batch_size = 10
    params._epochs = 1.0

    println("MODEL:" + params.fullName)
    val drf = new DeepLearning(params)
    drf.trainModel.get
  }

}

