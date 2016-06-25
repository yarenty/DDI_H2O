package com.yarenty.ddi

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable
import org.apache.spark.h2o._

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import water.parser._

//import org.apache.spark.h2o.{DoubleHolder, H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkFiles}
import water.support.SparkContextSupport

import com.yarenty.ddi.schemas._

/**
  * Created by yarenty on 23/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object DataMunging extends SparkContextSupport {


  val PROCESSED_DAY = "2016-01-01"
  val data_dir = "/opt/data/season_1/"
  val training_dir = data_dir + "training_data/"
  val test_dir = data_dir + "test_set_1/"
  val output_training_dir = data_dir + "outtrain/"
  val output_test_dir = data_dir + "outtest/"


  val order_csv = training_dir + "order_data/order_data_" + PROCESSED_DAY
  //2016-01-01
  val cluster_csv = training_dir + "cluster_map/cluster_map"
  val poi_csv = training_dir + "poi_data/poi_data"
  val traffic_csv = training_dir + "traffic_data/traffic_data_" + PROCESSED_DAY
  val weather_csv = training_dir + "weather_data/weather_data_" + PROCESSED_DAY


  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("DDI Data Munging")

    val conf = configure("H2O: DDI Data Munging")
    val sc = new SparkContext(conf)

    println(s"\n\n CONTEXT is HERE !!!!!!\n")

    // val h2oContext = H2OContext.getOrCreate(sc)
    val h2oContext = new H2OContext(sc).start()


    import h2oContext._
    import h2oContext.implicits._


    println(s"\n\n H2O CONTEXT is TOO !!!!!!\n")


    addFiles(sc,
      absPath(order_csv),
      absPath(cluster_csv),
      absPath(poi_csv),
      absPath(traffic_csv),
      absPath(weather_csv)
    )

    println(s"\n\n!!!!! FILES ADDED start CSV parser!!!!\n\n")


    // Use super-fast advanced H2O CSV parser !!!
    val orderData = new H2OFrame(OrderCSVParser.get, new File(SparkFiles.get("order_data_" + PROCESSED_DAY)))
    println(s"\n===> ORDERS via H2O#Frame#count: ${orderData.numRows()}\n")

    //  Use H2O to RDD transformation
    val orderTable = asRDD[Order](orderData)
    println(s"\n===> ORDERS in ${order_csv} via RDD#count call: ${orderTable.count()}\n")
    //@TODO: create accumulators ? can they be map "1_1_1" => "timeslot_startDistrict_destinationDistrict? if not 700K of them???
    //object VectorAccumulatorParam extends AccumulatorParam[Vector] ?





    //DISTRICT - simple parser
    val clusterData = sc.textFile(enforceLocalSparkFile("cluster_map"), 3).cache()
    println(s"\n===> DISTRICTS via H2O#Frame#count: ${clusterData.count()}\n")

    //@TODO: create as hashmap and broadcast it
    val districtMap = sc.accumulableCollection(mutable.HashMap[String, Int]())
    clusterData.map(_.split("\t")).map(row => {
      //val district = DistrictParse(row) // really not need this !
      val a = row(0)
      val b = row(1).trim.toInt
      println(s" adding: ${a} => ${b}")
      districtMap += (a ->b)
    }).count()   //force to execute
    println(s"\n===> DistrictMap:: ${districtMap.value.size} ")
    districtMap.value.foreach { case (k, v) => println(s" ${k} => ${v}") }

    //    val districtData = new H2OFrame(clusterTable)
    //    println(s"\n===> DISTRICTS via H2O#Frame#count: ${districtData.numRows()}\n")
    //    println(s"\n===> DISTRICTS in ${cluster_csv} via RDD#count call: ${clusterTable.count()}\n")



    // Use super-fast advanced H2O CSV parser !!!
    val trafficData = new H2OFrame(TrafficCSVParser.get, new File(SparkFiles.get("traffic_data_" + PROCESSED_DAY)))
    println(s"\n===> TRAFFIC via H2O#Frame#count: ${trafficData.numRows()}\n")

    //  Use H2O to RDD transformation
    val trafficTable: RDD[Traffic] = asRDD[TrafficIN](trafficData).map(row => TrafficParse(row)).filter(!_.isWrongRow())
    //val trafficTable = asRDD[Traffic](trafficData)
    println(s"\n===> TRAFFIC in ${order_csv} via RDD#count call: ${trafficTable.count()}\n")





    // Use super-fast advanced H2O CSV parser !!!
    val weatherData = new H2OFrame(WeatherCSVParser.get, new File(SparkFiles.get("weather_data_" + PROCESSED_DAY)))
    println(s"\n===> WEATHER via H2O#Frame#count: ${weatherData.numRows()}\n")

    //  Use H2O to RDD transformation
    val weatherTable = asRDD[Weather](weatherData)
    println(s"\n===> WEATHER in ${order_csv} via RDD#count call: ${weatherTable.count()}\n")




    //    parseFiles
    //      paths: ["/opt/data/season_1/training_data/poi_data/poi_data"]
    //      destination_frame: "poi_data.hex"
    //      parse_type: "CSV"
    //      separator: 9
    //      number_columns: 139
    //      single_quotes: false
    //      column_names: ["DistrictHash","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","",""]
    //      column_types: ["String","String","String","String","String","String","String","String","String","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","String","String","String","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","Enum","String","Enum","String","String","Enum","String","String","String","Enum","Enum","Enum","Enum","String","Enum","Enum","String","Enum","String","Enum","Enum","String","String","String","String","String","String","Enum","String","Enum","Enum","String","String","Enum","String","String","String","String","String","String","String","String","String","Enum","String","String","String","String","String","Enum","String","String","String","String","String","Enum","String","String","String","String","Enum","String","String","String","String","String","String","String","Enum","String","Enum","Enum","String","String","String","String","Enum","String","Enum","String","String","String","String","String","String","Enum","String","Enum","Enum","Enum","String","String","Enum","String","String","String","Enum","String","String","String","String","Enum","String","String","String","String"]
    //      delete_on_done: true
    //      check_header: -1
    //      chunk_size: 4194304

    // Use super-fast advanced H2O CSV parser !!!
    val poiData = new H2OFrame(new File(SparkFiles.get("poi_data")))
    println(s"\n===> POI via H2O#Frame#count: ${poiData.numRows()}\n")

    //  Use H2O to RDD transformation
    val poiTable = poiData.vecs()
    println(s"\n===> POI in ${order_csv} via RDD#count call: ${poiTable.length}\n")


    //val dl =   new DeepLearning()

    //val numAs = logData.filter(line => line.contains("a")).count()

    //// println("Lines with a: %s, Lines with b: %s".format(numAs))
  }


}