package com.yarenty.ddi

import java.io.{File, PrintWriter}

import org.apache.spark._
import org.apache.spark.h2o._
import water._
import water.fvec._

import scala.collection.mutable
import scala.collection.mutable.HashMap

//import org.apache.spark.{SparkFiles, h2o, SparkContext}
//import org.apache.spark.h2o.{RDD, H2OFrame, DoubleHolder, H2OContext}
import com.yarenty.ddi.schemas._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import water.support.SparkContextSupport


/**
  * Created by yarenty on 23/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
object DataMungingTest extends SparkContextSupport {


  var PROCESSED_DAY = "2016-01-01"
  val data_dir = "/opt/data/season_1/"
  val output_dir = "/opt/data/season_1/outdata/day_"


  val cluster_csv = data_dir + "test_set_1/cluster_map/cluster_map"
  val poi_csv = data_dir + "test_set_1/poi_data/poi_data"
  var order_csv = ""
  var traffic_csv = ""
  var weather_csv = ""


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)

    addFiles(h2oContext.sparkContext,
      absPath(cluster_csv),
      absPath(poi_csv)
    )

    println(s" Info about district added - start processing.")
    //will need distric id info everywhere - so load them and broadcast
    val disctrictMapBR = sc.broadcast(processDistricts(sc))


    // Use super-fast advanced H2O CSV parser !!!
    val poiData = new h2o.H2OFrame(new File(SparkFiles.get("poi_data")))
    println(s"\n===> POI via H2O#Frame#count: ${poiData.numRows()}\n")

    //  Use H2O to RDD transformation
    //    val poiTable = poiData.vecs()
    //    println(s"\n===> POI in ${order_csv} via RDD#count call: ${poiTable.length}\n")


    val poi: Map[Int, Map[String, Int]] = asRDD[POI](poiData).map(row => {
      getPOIMap(disctrictMapBR, row)
    }).collect().toMap


    val mergedPOI: Map[Int, Map[String, Int]] = mergePOI(poi)

    for (m <- mergedPOI) {
      println(m)
    }


    val files = {
      order_csv = data_dir + "training_data/order_data/order_data_"
      traffic_csv = data_dir + "training_data/traffic_data/traffic_data_"
      weather_csv = data_dir + "training_data/weather_data/weather_data_"

      val out: Seq[Tuple4[String, String, String, String]] =
        (1 to 21).map(i => {
          val pd = "2016-01-" + "%02d".format(i)
          (pd, order_csv + pd, traffic_csv + pd, weather_csv + pd)
        }).toSeq

      order_csv = data_dir + "test_set_1/order_data/order_data_"
      traffic_csv = data_dir + "test_set_1/traffic_data/traffic_data_"
      weather_csv = data_dir + "test_set_1/weather_data/weather_data_"
      val a = Array("2016-01-22_test", "2016-01-24_test", "2016-01-26_test", "2016-01-28_test", "2016-01-30_test")
      var x = 21

      out ++ a.map(pd => {
        x += 1
        (pd, order_csv + pd, traffic_csv + pd, weather_csv + pd)
      })


    }


    for (f <- files) {
      PROCESSED_DAY = f._1

      println(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\nSTART PROCESSING : ${PROCESSED_DAY}")

      addFiles(sc,
        absPath(f._2),
        absPath(f._3),
        absPath(f._4)
      )



      val orderData = new h2o.H2OFrame(OrderCSVParser.get,
        new File(SparkFiles.get("order_data_" + PROCESSED_DAY))) // Use super-fast advanced H2O CSV parser !!!
      println(s"\n===> ORDERS via H2O#Frame#count: ${orderData.numRows()}\n")
      val orderTable = asRDD[Order](orderData) //  Use H2O to RDD transformation
      println(s"\n===> ORDERS in ${f._2} via RDD#count call: ${orderTable.count()}\n")



      //get number of demand drives
      val orders: Map[Int, Int] = orderTable.map(row => {
        val timeslice = getTimeSlice(row.Time.get)
        var from = disctrictMapBR.value.get(row.StartDH.get)
        var to = disctrictMapBR.value.get(row.DestDH.get)
        if (to == None) to = Option(0)
        val indx = getIndex(timeslice, from.get, to.get)
        indx
      }).groupBy(identity).mapValues(_.size).collect().toMap

      //get number of gap drives (did not happen)
      val gaps: Map[Int, Int] = orderTable.map(row => {
        val timeslice = getTimeSlice(row.Time.get)
        var from = disctrictMapBR.value.get(row.StartDH.get)
        var to = disctrictMapBR.value.get(row.DestDH.get)
        if (to == None) to = Option(0)
        val indx = getIndex(timeslice, from.get, to.get)
        val gap = row.DriverId.get
        if (gap != "NULL") {
          0
        } else {
          indx
        }
      }).groupBy(identity).mapValues(_.size).collect().toMap

      println(s"\n===> ALL ORDERS :: ${orders.size} ")
      println(s"\n===> GAP ORDERS :: ${gaps.size} ")



      val trafficData = new h2o.H2OFrame(TrafficCSVParser.get,
        new File(SparkFiles.get("traffic_data_" + PROCESSED_DAY))) // Use super-fast advanced H2O CSV parser !!!
      println(s"\n===> TRAFFIC via H2O#Frame#count: ${trafficData.numRows()}\n")

      val trafficTable: h2o.RDD[Traffic] = asRDD[TrafficIN](trafficData)
        .map(row => TrafficParse(row))
        .filter(!_.isWrongRow())
      println(s"\n===> TRAFFIC in ${f._3} via RDD#count call: ${trafficTable.count()}\n")


      var traffic: Map[Int, Tuple4[Int, Int, Int, Int]] = trafficTable.map(row => {
        val ts = getTimeSlice(row.Time.get)
        if (ts < 1) println(s" WRONG TIME: ${row.Time} ")
        val din = disctrictMapBR.value.get(row.DistrictHash.get).get
        val t1 = row.Traffic1.get
        val t2 = row.Traffic2.get
        val t3 = row.Traffic3.get
        val t4 = row.Traffic4.get
        (ts * 100 + din) ->(t1, t2, t3, t4)
      }).collect().toMap
      println(s" TRAFFIC MAP SIZE: ${traffic.size}")


      var filledTraffic: Tuple4[Int, Int, Int, Int] = (0, 0, 0, 0)
      //fill traffic
      for (din <- 1 to 66) {
        for (i <- 1 to 144) {
          val idx = i * 100 + din
          if (traffic.contains(idx)) {
            filledTraffic = traffic.get(idx).get
          }
        }
        for (i <- 1 to 144) {
          val idx = i * 100 + din
          if (traffic.contains(idx)) {
            filledTraffic = traffic.get(idx).get
          } else {
            traffic += idx -> filledTraffic
          }
        }
      }
      println(s" TRAFFIC MAP SIZE AFTER FILL: ${traffic.size}")


      val weatherData = new h2o.H2OFrame(WeatherCSVParser.get,
        new File(SparkFiles.get("weather_data_" + PROCESSED_DAY))) // Use super-fast advanced H2O CSV parser !!!
      println(s"\n===> WEATHER via H2O#Frame#count: ${weatherData.numRows()}\n")
      val weatherTable: h2o.RDD[Weather] = asRDD[WeatherIN](weatherData)
        .map(row => WeatherParse(row)).filter(!_.isWrongRow)
      println(s"\n===> WEATHER in ${f._4} via RDD#count call: ${weatherTable.count()}\n")


      var weather: Map[Int, Tuple3[Int, Double, Double]] = weatherTable.map(row => {
        row.ts ->(
          row.Weather.get,
          if (row.Temperature.get < 0) 0.0 else row.Temperature.get, //test set has -6 while train always +
          row.Pollution.get)

      }).collect().toMap
      println(s" WEATHER MAP SIZE: ${weather.size}")

      var filledWeather: Tuple3[Int, Double, Double] = (0, 0, 0) //after doing naive bayes - this looks much better ;-)
      for (i <- 1 to 144) {
        if (weather.contains(i)) {
          filledWeather = weather.get(i).get
        }
      }
      for (i <- 1 to 144) {
        if (weather.contains(i)) {
          filledWeather = weather.get(i).get
        } else {
          weather += i -> filledWeather
        }
      }
      println(s" WEATHER MAP SIZE AFTER FILL: ${weather.size}")



      //      "1", "1#1", "1#2", "1#3", "1#4", "1#5", "1#6", "1#7", "1#8", "1#9", "1#10", "1#11",
      //      "2#1", "2#2", "2#3", "2#4", "2#5", "2#6", "2#7", "2#8", "2#9", "2#10", "2#11", "2#12", "2#13",
      //      "3", "3#1", "3#2", "3#3", "3#4", "3#5",
      //      "4", "4#1", "4#2", "4#3", "4#4", "4#5", "4#6", "4#7", "4#8", "4#9", "4#10", "4#11", "4#12", "4#13", "4#14", "4#15", "4#16", "4#17", "4#18",
      //      "5", "5#1", "5#2", "5#3", "5#4",
      //      "6", "6#1", "6#2", "6#3", "6#4",
      //      "7", "7#1", "7#2", "7#3",
      //      "8", "8#1", "8#2", "8#3", "8#4", "8#5",
      //      "10#1",
      //      "11", "11#1", "11#2", "11#3", "11#4", "11#5", "11#6", "11#7", "11#8",
      //      "12",
      //      "13#1", "13#3", "13#4", "13#5", "13#6", "13#8",
      //      "14", "14#1", "14#2", "14#3", "14#4", "14#5", "14#6", "14#7", "14#8", "14#9", "14#10",
      //      "15", "15#1", "15#2", "15#3", "15#4", "15#5", "15#6", "15#7", "15#8",
      //      "16", "16#1", "16#2", "16#3", "16#4", "16#5", "16#6", "16#7", "16#8", "16#9", "16#10", "16#11", "16#12",
      //      "17", "17#1", "17#2", "17#3", "17#4", "17#5",
      //      "18",
      //      "19", "19#1", "19#2", "19#3", "19#4",
      //      "20", "20#1", "20#2", "20#3", "20#4", "20#5", "20#6", "20#7", "20#8", "20#9",
      //      "21#1", "21#2", "21#4",
      //      "22", "22#1", "22#2", "22#3", "22#4", "22#5", "22#6",
      //      "23", "23#1", "23#2", "23#3", "23#4", "23#5", "23#6",
      //      "24", "24#1", "24#2", "24#3",
      //      "25", "25#1", "25#2", "25#3", "25#4", "25#5", "25#6", "25#7", "25#8", "25#9"


      val headers = Array("id", "timeslice", "districtID", "destDistrict", "demand", "gap",
        "traffic1", "traffic2", "traffic3", "traffic4",
        "weather", "temp", "pollution",
        "1", "2", "3", "4", "5", "6", "7", "8", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25"
      )
      val types = Array(Vec.T_NUM, Vec.T_CAT, Vec.T_CAT, Vec.T_CAT, Vec.T_NUM, Vec.T_NUM,
        Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM,
        Vec.T_CAT, Vec.T_NUM, Vec.T_NUM,
        Vec.T_NUM)

      val myData = new h2o.H2OFrame(lineBuilder(headers, types,
        orders,
        gaps,
        traffic,
        weather,
        mergedPOI)) //poi

      val v = DKV.put(myData)

      println(s" AND MY DATA IS: ${myData.key} =>  ${myData.numCols()} / ${myData.numRows()}")
      println(s" frame::${v}")


      val csv = myData.toCSV(true, false)

      val csv_writer = new PrintWriter(new File(output_dir + PROCESSED_DAY))
      while (csv.available() > 0) {
        csv_writer.write(csv.read.toChar)
      }
      csv_writer.close

      println(
        s"""
           |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
           |!!  OUTPUT CREATED: ${output_dir + PROCESSED_DAY} !!
           |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         """.stripMargin)


      //clean
      myData.delete()
      trafficTable.delete()
      trafficData.delete()
      weatherTable.delete()
      weatherData.delete()
      orderTable.delete()
      orderData.delete()

      println("... and cleaned")
    }


  }


  def lineBuilder(headers: Array[String], types: Array[Byte],
                  orders: Map[Int, Int],
                  gaps: Map[Int, Int],
                  traffic: Map[Int, Tuple4[Int, Int, Int, Int]],
                  weather: Map[Int, Tuple3[Int, Double, Double]],
                  poi: Map[Int, Map[String, Int]]): Frame = {

    val len = headers.length

    val fs = new Array[Futures](len)
    val av = new Array[AppendableVec](len)
    val chunks = new Array[NewChunk](len)
    val vecs = new Array[Vec](len)


    for (i <- 0 until len) {
      fs(i) = new Futures()
      av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_NUM)
      chunks(i) = new NewChunk(av(i), 0)
    }

    for (ts <- 1 to 144) {
      for (din <- 1 to 66) {
        for (dout <- 0 to 66) {
          val idx = getIndex(ts, din, dout)
          chunks(0).addNum(idx)
          chunks(1).addNum(ts)
          chunks(2).addNum(din)
          chunks(3).addNum(dout)

          if (orders.contains(idx)) {
            chunks(4).addNum(orders.get(idx).get)
          }
          else {
            chunks(4).addNum(0)
          }


          if (gaps.contains(idx)) {
            chunks(5).addNum(gaps.get(idx).get)
          }
          else {
            chunks(5).addNum(0)
          }

          var tidx = ts * 100 + din
          if (traffic.contains(tidx)) {
            chunks(6).addNum(traffic.get(tidx).get._1)
            chunks(7).addNum(traffic.get(tidx).get._2)
            chunks(8).addNum(traffic.get(tidx).get._3)
            chunks(9).addNum(traffic.get(tidx).get._4)
          }
          else {
            chunks(6).addNA()
            chunks(7).addNA()
            chunks(8).addNA()
            chunks(9).addNA()
          }

          if (weather.contains(ts)) {
            chunks(10).addNum(weather.get(ts).get._1)
            chunks(11).addNum(weather.get(ts).get._2.toDouble)
            chunks(12).addNum(weather.get(ts).get._3.toDouble)
          } else {
            chunks(10).addNA()
            chunks(11).addNA()
            chunks(12).addNA()
          }

          for (pp <- 13 until len) {

            if (poi.contains(din)) {
              val m = poi.get(din).get

              if (m.contains(headers(pp))) {
                chunks(pp).addNum(m.get(headers(pp)).get)
              }
              else {
                chunks(pp).addNum(0)
              }
            }
            else {
              chunks(pp).addNA()

            }
          }


        }
      }
    }

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }

    val key = Key.make("TimedOrders")
    return new Frame(key, headers, vecs)

  }


  /**
    * Return map of POIs .
    *
    * @param disctrictMapBR
    * @param row
    * @return (DisctrictID -> Map [Category, HowMany]])
    */
  def getPOIMap(disctrictMapBR: Broadcast[mutable.HashMap[String, Int]], row: POI): (Int, Map[String, Int]) = {
    val iter = row.productIterator
    val district: String = iter.next match {
      case None => ""
      case Some(value) => value.toString // value is of type String
    }

    val din: Int = disctrictMapBR.value.get(district).get
    var m: Map[String, Int] = Map[String, Int]()
    while (iter.hasNext) {
      val col = iter.next match {
        case None => ""
        case Some(value) => value.toString // value is of type String
      }

      if (!col.isEmpty && col != "") {
        val v = col.split(":")
        m += (v(0) -> v(1).toInt)
      }
    }
    din -> m
  }

  /**
    * Extremely simple PCA ;-)
    * Merge all POI sub categories into simple 1.
    *
    * @param poi
    * @return
    */
  def mergePOI(poi: Map[Int, Map[String, Int]]): Map[Int, Map[String, Int]] = {
    poi.map(row => {
      val idx = row._1
      val old = row._2
      var now: Map[String, Int] = Map[String, Int]()

      for (i <- 1 to 25) {
        var tmp = 0
        val im = s"${i}"
        if (old.contains(im)) {
          tmp += old.get(im).get
        }
        for (j <- 1 to 20) {
          val id = s"${i}#${j}"
          if (old.contains(id)) {
            tmp += old.get(id).get
          }
        }
        now += (s"${i}" -> tmp)
      }

      idx -> now
    })
  }


  /**
    * Return index of time slice from date - 10 min period
    *
    * @param t
    * @return
    */
  def getTimeSlice(t: String): Int = {
    val tt = t.split(" ")(1).split(":")
    return ((tt(0).toInt * 60 * 60 + tt(1).toInt * 60 + tt(2).toInt) / (10 * 60)) + 1
  }

  /**
    * Create index based on
    *
    * @param t timeslice
    * @param s start district
    * @param d destination district
    * @return
    */
  def getIndex(t: Int, s: Int, d: Int): Int = {
    return t * 10000 + s * 100 + d
  }

  /**
    * Getting back - timeslice, start district, destination district - from index
    *
    * @param i index
    * @return
    */
  def getBack(i: Int): (Int, Int, Int) = {
    val i1 = i / 10000
    val i2 = (i - (i1 * 10000)) / 100
    val i3 = i - (i1 * 10000) - i2 * 100
    return (i1, i2, i3)
  }


  /**
    * Parse districts data -> collect them and then redistribute.
    *
    * @param sc
    * @return
    */
  def processDistricts(sc: SparkContext): mutable.HashMap[String, Int] = {
    //DISTRICT - simple parser
    val clusterData = sc.textFile(enforceLocalSparkFile("cluster_map"), 3).cache()
    val districtMap = sc.accumulableCollection(HashMap[String, Int]())
    clusterData.map(_.split("\t")).map(row => {
      districtMap += (row(0) -> row(1).trim.toInt)
    }).count()
    println(s"===> DistrictMap size:: ${districtMap.value.size} ")
    districtMap.value.foreach { case (k, v) => println(" \""+k + "\" -> "+v+",") }
    districtMap.value
  }
}
