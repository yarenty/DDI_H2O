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

    //    parseFiles
    //      paths: ["/opt/data/season_1/training_data/order_data/order_data_2016-01-01"]
    //      destination_frame: "order_data_2016_01_01.hex"
    //      parse_type: "CSV"
    //      separator: 9
    //      number_columns: 7
    //      single_quotes: false
    //      column_names: ["oid","did","pid","stDH","deDH","price","time"]
    //      column_types: ["String","String","String","String","String","Numeric","String"]
    //      delete_on_done: true
    //      check_header: -1
    //      chunk_size: 930182
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array("OrderId", "DriverId", "PassengerId", "StartDH", "DestDH", "Price", "Time")
    val orderTypes = ParseSetup.strToColumnTypes(Array("string", "string", "string", "string", "string", "float", "string"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setSeparator('\t')
    parseOrders.setNumberColumns(7)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(-1)


    // Use super-fast advanced H2O CSV parser !!!
    val orderData = new H2OFrame(parseOrders, new File(SparkFiles.get("order_data_" + PROCESSED_DAY)))
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
    val districtMap = sc.accumulableCollection(mutable.HashMap[String,Int]())
    clusterData.map(_.split("\t")).map(row => {
      val district = DistrictParse(row)
      districtMap += (district.DistrictHash.get -> district.DistrictID.get)
    })
    println(s"\n===> DistrictMap:: ${districtMap} ")

//    val districtData = new H2OFrame(clusterTable)
//    println(s"\n===> DISTRICTS via H2O#Frame#count: ${districtData.numRows()}\n")
//    println(s"\n===> DISTRICTS in ${cluster_csv} via RDD#count call: ${clusterTable.count()}\n")







//    parseFiles
//      paths: ["/opt/data/season_1/training_data/traffic_data/traffic_data_2016-01-01"]
//      destination_frame: "traffic_data_2016_01_01.hex"
//      parse_type: "CSV"
//      separator: 9
//      number_columns: 6
//      single_quotes: false
//      column_names: ["DistrictHash","Traffic1","Traffic2","Traffic3","Traffic4","Time"]
//      column_types: ["String","String","String","String","String","String"]
//      delete_on_done: true
//      check_header: -1
//      chunk_size: 7220
    val parseTraffic: ParseSetup = new ParseSetup()
    val trafficNames: Array[String] = Array("DistrictHash","Traffic1","Traffic2","Traffic3","Traffic4","Time")
    val trafficTypes = ParseSetup.strToColumnTypes(Array("string", "string", "string", "string", "string", "string"))
    parseTraffic.setColumnNames(trafficNames)
    parseTraffic.setColumnTypes(trafficTypes)
    parseTraffic.setParseType(DefaultParserProviders.CSV_INFO)
    parseTraffic.setSeparator('\t')
    parseTraffic.setNumberColumns(6)
    parseTraffic.setSingleQuotes(false)
    parseTraffic.setCheckHeader(-1)

    // Use super-fast advanced H2O CSV parser !!!
    val trafficData = new H2OFrame(parseTraffic, new File(SparkFiles.get("traffic_data_" + PROCESSED_DAY)))
    println(s"\n===> TRAFFIC via H2O#Frame#count: ${trafficData.numRows()}\n")

    //  Use H2O to RDD transformation
    val trafficTable:RDD[Traffic] = asRDD[TrafficIN](trafficData).map(row => TrafficParse(row)).filter(!_.isWrongRow())
    //val trafficTable = asRDD[Traffic](trafficData)
    println(s"\n===> TRAFFIC in ${order_csv} via RDD#count call: ${trafficTable.count()}\n")



//    parseFiles
//      paths: ["/opt/data/season_1/training_data/weather_data/weather_data_2016-01-01"]
//      destination_frame: "weather_data_2016_01_01.hex"
//      parse_type: "CSV"
//      separator: 9
//      number_columns: 4
//      single_quotes: false
//      column_names: ["time","weather","temperature","pollution"]
//      column_types: ["String","Numeric","Numeric","Numeric"]
//      delete_on_done: true
//      check_header: -1
//      chunk_size: 4194304
    val parseWeather: ParseSetup = new ParseSetup()
    val weatherNames: Array[String] = Array("Time","Weather","Temperature","Pollution")
    val weatherTypes = ParseSetup.strToColumnTypes(Array("string", "int", "float", "float"))
    parseWeather.setColumnNames(weatherNames)
    parseWeather.setColumnTypes(weatherTypes)
    parseWeather.setParseType(DefaultParserProviders.CSV_INFO)
    parseWeather.setSeparator('\t')
    parseWeather.setNumberColumns(6)
    parseWeather.setSingleQuotes(false)
    parseWeather.setCheckHeader(-1)

    // Use super-fast advanced H2O CSV parser !!!
    val weatherData = new H2OFrame(parseWeather, new File(SparkFiles.get("weather_data_" + PROCESSED_DAY)))
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