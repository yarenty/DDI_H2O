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


  val test_imp_dir = "/opt/data/season_1/outdatanorm/day_"
  val train_imp_dir = "/opt/data/season_1/outdatanorm/day_"


  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    println(s"\n\n LETS MODEL\n")

    val testset = Array("2016-01-22_test", "2016-01-26_test", "2016-01-30_test", "2016-01-28_test", "2016-01-24_test")
    //val testset = Array("2016-01-22_test")
    var trainset  = (1 to 21).map(i => "2016-01-" + "%02d".format(i)).toArray

    for (p <- testset) addFiles(sc, absPath(test_imp_dir + p))
    for (p <- trainset) addFiles(sc, absPath(train_imp_dir + p))

    trainset  = (2 to 21).map(i => "2016-01-" + "%02d".format(i)).toArray
    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("day_" + a))).toSeq
    val trainURIs = trainset.map(a => new URI("file:///" + SparkFiles.get("day_"+ a))).toSeq

    // Use super-fast advanced H2O CSV parser !!!
    var smOutputTrain = new h2o.H2OFrame(SMOutputCSVParser.get, new URI("file:///" + SparkFiles.get("day_2016-01-01")))

    var df = asDataFrame(smOutputTrain)

    println(s" SIZE: ${df.count} ");
    //1 by 1 to avoid OOM!
    for (tu <- trainURIs) {

      val tmp = new h2o.H2OFrame(SMOutputCSVParser.get, tu)

      df = df.unionAll(asDataFrame(tmp))
      println(s" SIZE: ${df.count} ");

    }
    println(s" SIZE: ${df.count} ");


    val outputTrain = asH2OFrame(df, "train")
    outputTrain.colToEnum(Array("timeslice","districtID","destDistrict","weather"))

    println(s" TRAIN DATA CREATED ");
    //val smOutputTrain = new h2o.H2OFrame(SMOutputCSVParser.get, trainURIs: _*)
    val smOutputTest = new h2o.H2OFrame(SMOutputCSVParser.get, testURIs: _*)

    smOutputTest.colToEnum(Array("timeslice","districtID","destDistrict","weather"))

    println(s" TEST DATA CREATED ");

    println(s"\n===> TRAIN: ${outputTrain.numRows()}\n")
    println(s"\n===> TEST: ${smOutputTest.numRows()}\n")


    val gapModel = drfGapOnlyModel(outputTrain, smOutputTest)
    // SAVE THE MODEL!!!
    val om = new FileOutputStream("/opt/data/GRFGAPOnlyModel_" + System.currentTimeMillis() + ".java")
    gapModel.toJava(om, false, false)




    for (u <- testURIs) {

      val predictMe = new h2o.H2OFrame(SMOutputCSVParser.get, u)

      predictMe.colToEnum(Array("timeslice","districtID","destDistrict","weather"))

      val predict = gapModel.score(predictMe)
      val vec = predict.get.lastVec

      println("OUT VECTOR:" + vec.length)

      predictMe.add("predict", vec)

      saveOutput(predictMe,u.toString)

    }


    //      outFrame.delete()
    //      predict.delete()
    // model.deviance()


    println("=========> off to go!!!")


  }


  def saveOutput(smOutputTest: H2OFrame, fName:String): Unit = {

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

    val toSee = new H2OFrame(o)

    println(s" output should be visible now ")


    //      val p = u.getPath.split("/")
    //      val n = p(p.length - 1)

    val n = fName.split("/")
    val name = n(n.length-1)
    val csv = o.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/out/"+name+".csv"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close

    println(s" CSV created: /opt/data/season_1/out/"+name+".csv")

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
    params._ignored_columns = Array("id", "gap")
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
    params._ignored_columns = Array("id", "olddemand")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val drf = new DRF(params)


    println("DRF:" + drf)

    drf.trainModel.get

  }


  //buildModel 'drf', {"model_id":"drf-82bdab1a-0ea4-4546-8b12-fe16dd84e33c",
  // "training_frame":"train",
  // "validation_frame":"day_2016_01_22_test.hex",
  // "nfolds":0,"response_column":"gap","ignored_columns":["id","demand"],
  // "ignore_const_cols":true,"ntrees":"100","max_depth":20,"min_rows":1,
  // "nbins":20,"seed":-1,"mtries":-1,"sample_rate":0.6320000290870667,
  // "score_each_iteration":false,"score_tree_interval":0,"nbins_top_level":1024,
  // "nbins_cats":1024,"r2_stopping":0.999999,"stopping_rounds":0,"stopping_metric":"AUTO",
  // "stopping_tolerance":0.001,"max_runtime_secs":0,"checkpoint":"","col_sample_rate_per_tree":1,
  // "min_split_improvement":0,"histogram_type":"AUTO","build_tree_one_node":false,"sample_rate_per_class":[],
  // "binomial_double_trees":false,"col_sample_rate_change_per_level":1}


  def drfGapOnlyModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    // params._distribution = Distribution.Family.gaussian

    params._ntrees = 20
    params._response_column = "gap"
    params._ignored_columns = Array("id", "demand")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val drf = new DRF(params)


    println("DRF:" + drf)

    drf.trainModel.get

  }

  //  buildModel 'deeplearning',
  // {"model_id":"deeplearning-c93159b7-5e6a-4ab3-a841-ff44a1bc9d03",
  // "training_frame":"day_2016_01_01.hex","validation_frame":"day_2016_01_22_test.hex",
  // "nfolds":0,
  // "response_column":"demand",
  // "ignored_columns":["id","gap"],
  // "ignore_const_cols":true,
  // "activation":"Rectifier",
  // "hidden":[50,50],"epochs":10,
  // "variable_importances":false,
  // "score_each_iteration":false,
  // "checkpoint":"","use_all_factor_levels":true,"standardize":true,
  // "train_samples_per_iteration":-2,"adaptive_rate":true,"input_dropout_ratio":0,
  // "l1":0,"l2":0,
  // "loss":"Automatic","distribution":"AUTO",
  // "score_interval":5,"score_training_samples":10000,"score_validation_samples":0,"score_duty_cycle":0.1,
  // "stopping_rounds":5,"stopping_metric":"AUTO","stopping_tolerance":0,"max_runtime_secs":0,
  // "autoencoder":false,"pretrained_autoencoder":"","overwrite_with_best_model":true,
  // "target_ratio_comm_to_comp":0.05,"seed":-5642090231507718000,"rho":0.99,"epsilon":1e-8,
  // "max_w2":"Infinity","initial_weight_distribution":"UniformAdaptive","regression_stop":0.000001,
  // "diagnostics":true,"fast_mode":true,"force_load_balance":true,"single_node_mode":false,
  // "shuffle_training_data":false,"missing_values_handling":"MeanImputation","quiet_mode":false,
  // "sparse":false,"col_major":false,"average_activation":0,"sparsity_beta":0,"max_categorical_features":2147483647,
  // s"reproducible":false,"export_weights_and_biases":false,"mini_batch_size":1,"elastic_averaging":false}


}

