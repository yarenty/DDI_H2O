package com.yarenty.ddi.traffic

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import com.yarenty.ddi.MLProcessor
import com.yarenty.ddi.MLProcessor.h2oContext._
import com.yarenty.ddi.MLProcessor.h2oContext.implicits._
import com.yarenty.ddi.schemas.OutputLine
import hex.glm.{GLM, GLMModel}
import hex.glm.GLMModel.GLMParameters
import hex.naivebayes.NaiveBayesModel.NaiveBayesParameters
import hex.naivebayes.{NaiveBayes, NaiveBayesModel}
import hex.tree.drf.{DRF, DRFModel}
import hex.tree.drf.DRFModel.DRFParameters
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.{SparkFiles, h2o}
import water.Key
import water.fvec.Frame
import water.support.SparkContextSupport

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object TrafficPredic extends SparkContextSupport {


  val test_imp_dir = "/opt/data/season_1/outtraffic/t_"
  val train_imp_dir = "/opt/data/season_1/outtraffic/t_"


  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext





    println(s"\n\n LETS MODEL\n")
    val predictset = Array("2016-01-22_predict", "2016-01-26_predict", "2016-01-30_predict", "2016-01-28_predict", "2016-01-24_predict")

    val testset = Array("2016-01-22_test", "2016-01-26_test", "2016-01-30_test", "2016-01-28_test", "2016-01-24_test")
    for (p <- testset) addFiles(sc, absPath(test_imp_dir + p))
    for (p <- predictset) addFiles(sc, absPath(test_imp_dir + p))
    for (i <- 1 to 21) addFiles(sc, absPath(train_imp_dir + "2016-01-" + "%02d".format(i)))

    val testURIs = testset.map(a => new URI("file:///" + SparkFiles.get("t_" + a))).toSeq
    val trainURIs = (1 to 21).map(a => new URI("file:///" + SparkFiles.get("t_2016-01-" + "%02d".format(a)))).toSeq

    // Use super-fast advanced H2O CSV parser !!!
    val smOutputTrain = new h2o.H2OFrame(TPCSVParser.get, trainURIs: _*)
    val smOutputTest = new h2o.H2OFrame(TPCSVParser.get, testURIs: _*)

    smOutputTrain.colToEnum(Array("day","timeslice", "district"))
    smOutputTest.colToEnum(Array("day","timeslice", "district"))


    println(s"\n===> TRAIN: ${smOutputTrain.numRows()}\n")
    println(s"\n===> TEST: ${smOutputTest.numRows()}\n")



    val t1Model = glmT1Model(smOutputTrain, smOutputTest)
    val t2Model = glmT2Model(smOutputTrain, smOutputTest)
    val t3Model = glmT3Model(smOutputTrain, smOutputTest)
    val t4Model = glmT4Model(smOutputTrain, smOutputTest)



    for (pu <- predictset) {
      val smOutputPredict = new h2o.H2OFrame(TPCSVParser.get, new URI("file:///" + SparkFiles.get("t_" + pu)))
      smOutputPredict.colToEnum(Array("day","timeslice", "district"))
      val predictT1Demand = t1Model.score(smOutputPredict)
      val vecT1 = predictT1Demand.get.lastVec
      smOutputPredict.add("t1p", vecT1)

      val predictT2Demand = t2Model.score(smOutputPredict)
      val vecT2 = predictT2Demand.get.lastVec
      smOutputPredict.add("t2p", vecT2)

      val predictT3Demand = t3Model.score(smOutputPredict)
      val vecT3 = predictT3Demand.get.lastVec
      smOutputPredict.add("t3p", vecT3)

      val predictT4Demand = t4Model.score(smOutputPredict)
      val vecT4 = predictT4Demand.get.lastVec
      smOutputPredict.add("t4p", vecT4)

      saveOutput(smOutputPredict,pu)
    }
    println("=========> off to go!!!")
  }


  def saveOutput(smOutputTest: H2OFrame, p: String): Unit = {

    import MLProcessor.sqlContext
    val names = Array("district", "timeslice", "t1", "t2","t3","t4","t1p","t2p","t3p","t4p")

    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val out = new Frame(key, names, smOutputTest.vecs(names))


    val csv = out.toCSV(true, false)
    val csv_writer = new PrintWriter(new File("/opt/data/season_1/outtraffic/t_"+p.substring(0,10)+"_test_all"))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close
  }


  //  buildModel 'naivebayes',
  // {"model_id":"naivebayes-4eca5924-44d7-41d3-9400-3ca7a8b872c1",
  // "nfolds":0,"training_frame":"w_2016_01_01.hex",
  // "validation_frame":"w_2016_01_22_test.hex",
  // "response_column":"Weather",
  // "ignored_columns":["Temp","Pollution"],
  // "ignore_const_cols":false,
  // "laplace":0,"min_sdev":0.001,
  // "eps_sdev":0,"min_prob":0.001,"eps_prob":0,"compute_metrics":false,
  // "score_each_iteration":false,"max_confusion_matrix_size":20,"
  // max_hit_ratio_k":0,"max_runtime_secs":0,"seed":0}


  def glmT1Model(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._response_column = "t1"
    params._ignored_columns = Array("t2", "t3","t4")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val nb = new DRF(params)
    nb.trainModel.get
  }

  def glmT2Model(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._response_column = "t2"
    params._ignored_columns = Array("t1", "t3","t4")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val nb = new DRF(params)
    nb.trainModel.get
  }

  def glmT3Model(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._response_column = "t3"
    params._ignored_columns = Array("t2", "t1","t4")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val nb = new DRF(params)
    nb.trainModel.get
  }

  def glmT4Model(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._response_column = "t4"
    params._ignored_columns = Array("t2", "t3","t1")
    params._ignore_const_cols = true

    println("PARAMS:" + params)
    val nb = new DRF(params)
    nb.trainModel.get
  }


}

