package com.yarenty.ddi.traffic

import java.io.{PrintWriter, File}


import com.yarenty.ddi.raw.DataMunging
import DataMunging._
import com.yarenty.ddi.schemas._
import com.yarenty.ddi.traffic.TrafficPredictionTrain._
import com.yarenty.ddi.traffic.TrafficPredictionTrain.data_dir
import com.yarenty.ddi.traffic.TrafficPredictionTrain.getTimeSlice
import com.yarenty.ddi.traffic.TrafficPredictionTrain.lineBuilder
import com.yarenty.ddi.traffic.TrafficPredictionTrain.output_dir
import com.yarenty.ddi.traffic.TrafficPredictionTrain.traffic_csv
import com.yarenty.ddi.traffic.TrafficPredictionTrain.weather_csv
import com.yarenty.ddi.utils.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkFiles, h2o}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import water.{Key, Futures}
import water.fvec.{Vec, NewChunk, AppendableVec, Frame}
import water.support.SparkContextSupport

import scala.collection.mutable

/**
  * Created by yarenty on 06/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object TrafficPrediction extends SparkContextSupport {

  val data_dir = "/opt/data/season_1/"
  val output_dir = "/opt/data/season_1/outtraffic/t_2016-01-"

  var traffic_csv = ""
  var weather_csv = ""




  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    addFiles(h2oContext.sparkContext,
      absPath(poi_csv)
    )


    // Use super-fast advanced H2O CSV parser !!!
    val poiData = new h2o.H2OFrame(new File(SparkFiles.get("poi_data")))
    println(s"\n===> POI via H2O#Frame#count: ${poiData.numRows()}\n")
    val poi: Map[Int, Map[String, Int]] = asRDD[POI](poiData).map(row => {
      getPOIMap(row)
    }).collect().toMap
    val mergedPOI: Map[Int, Map[String, Double]] = mergePOI(poi)
    for (m <- mergedPOI) {
      println(m)
    }



    val files = {

      weather_csv = data_dir + "outweather/w_"

      traffic_csv = data_dir + "test_set_1/traffic_data/traffic_data_"
      val a = Array("2016-01-22_test", "2016-01-24_test", "2016-01-26_test", "2016-01-28_test", "2016-01-30_test")
      var x = 20

      // out ++
      a.map(pd => {
        x += 2
        (x, traffic_csv + pd, weather_csv + pd, pd)
      })


    }


    for (f <- files) {
      val PROCESSED_DAY = f._1

      println(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\nSTART PROCESSING : ${PROCESSED_DAY}")

      addFiles(sc,
        absPath(f._2), absPath(f._3)
      )


      val trafficData = new h2o.H2OFrame(TrafficCSVParser.get,
        new File(SparkFiles.get("traffic_data_" + f._4))) // Use super-fast advanced H2O CSV parser !!!
      println(s"\n===> TRAFFIC via H2O#Frame#count: ${trafficData.numRows()}\n")



      val trafficTable: h2o.RDD[Traffic] = asRDD[TrafficIN](trafficData)
        .map(row => TrafficParse(row))
        .filter(!_.isWrongRow())


      val traffic: Map[Int, Tuple7[Int, Int, Int, Int, Int, Int, Int]] = trafficTable.map(row => {
        val ts = getTimeSlice(row.Time.get)
        if (ts < 1) println(s" WRONG TIME: ${row.Time} ")
        val din = Utils.districts.get(row.DistrictHash.get).get
        val t1 = row.Traffic1.get
        val t2 = row.Traffic2.get
        val t3 = row.Traffic3.get
        val t4 = row.Traffic4.get
        ts * 100 + din ->(PROCESSED_DAY % 7, din, ts, t1, t2, t3, t4)
      }).collect().toMap
      println(s" TRAFFIC MAP SIZE: ${traffic.size}")

      val normalizedTraffic: Map[Int, Tuple7[Int, Int, Int, Double, Double, Double, Double]] = traffic.map(x =>
        x._1 ->(x._2._1, x._2._2, x._2._3, x._2._4.toDouble / 2000.0, x._2._5.toDouble / 1000.0, x._2._6.toDouble / 400.0, x._2._7.toDouble / 200.0)
      )


      val weatherData = new h2o.H2OFrame(WCVSParser.get,
        new File(SparkFiles.get("w_" + f._4))) // Use super-fast advanced H2O CSV parser !!!
      println(s"\n===> WEATHER via H2O#Frame#count: ${weatherData.numRows()}\n")
      val weatherTable = asRDD[PWeather](weatherData)


      var weather: Map[Int, Tuple3[Int, Double, Double]] = weatherTable.map(row => {
        row.timeslice.get ->(
          row.timeslice.get,
          (20.0 + row.temp.get) / 40.0,
          row.pollution.get / 100.0)
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



      val headers = Array("day", "district", "timeslice", "t1", "t2", "t3", "t4", "temp", "pollution",
        "1", "2", "3", "4", "5", "6", "7", "8", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25"
      )


      val out = new h2o.H2OFrame(lineBuilder(headers, normalizedTraffic, weather, mergedPOI, PROCESSED_DAY))

      val csv = out.toCSV(true, false)

      val csv_writer = new PrintWriter(new File(output_dir + "%02d".format(PROCESSED_DAY) + "_predict"))
      while (csv.available() > 0) {
        csv_writer.write(csv.read.toChar)
      }
      csv_writer.close

      out.delete()
      trafficTable.delete()
      trafficData.delete()
    }


  }

  def lineBuilder(headers: Array[String],
                  traffic: Map[Int, Tuple7[Int, Int, Int, Double, Double, Double, Double]],
                  weather: Map[Int, Tuple3[Int, Double, Double]],
                  poi: Map[Int, Map[String, Double]],
                  pd: Int): Frame = {

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

    for (ts <- 1 to 144)
      for (din <- 1 to 66) {

        if (traffic.contains(ts * 100 + din)) {

          val t = traffic.get(ts * 100 + din).get

          chunks(0).addNum(t._1)
          chunks(1).addNum(t._2)
          chunks(2).addNum(t._3)
          chunks(3).addNum(t._4)
          chunks(4).addNum(t._5)
          chunks(5).addNum(t._6)
          chunks(6).addNum(t._7)
          chunks(7).addNum(weather.get(ts).get._2)
          chunks(8).addNum(weather.get(ts).get._3)
        } else {
          chunks(0).addNum(pd % 7)
          chunks(1).addNum(din)
          chunks(2).addNum(ts)
          chunks(3).addNA()
          chunks(4).addNA()
          chunks(5).addNA()
          chunks(6).addNA()
          chunks(7).addNum(weather.get(ts).get._2)
          chunks(8).addNum(weather.get(ts).get._3)
        }

        for (pp <- 9 until len) {

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

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }
    val key = Key.make("Traffic")
    return new Frame(key, headers, vecs)
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
    * Return map of POIs .
    * @param row
    * @return (DisctrictID -> Map [Category, HowMany]])
    */
  def getPOIMap(row: POI): (Int, Map[String, Int]) = {
    val iter = row.productIterator
    val district: String = iter.next match {
      case None => ""
      case Some(value) => value.toString // value is of type String
    }

    val din: Int = Utils.districts.get(district).get
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
  def mergePOI(poi: Map[Int, Map[String, Int]]): Map[Int, Map[String, Double]] = {
    poi.map(row => {
      val idx = row._1
      val old = row._2
      var now: Map[String, Double] = Map[String, Double]()

      val normalization: Array[Double] = Array(0.0,
        86071.0 / 12.0, 30544.0 / 13.0, 6640.0 / 6.0, 152803.0 / 18.0, 21165.0 / 5.0,
        61005.0 / 5.0, 35026.0 / 4.0, 91217.0 / 6.0, 1.0, 25.0, //#9 not exist
        419980.0 / 9.0, 166.0, 85739.0 / 6.0, 40172.0 / 10.0, 141515.0 / 9.0,
        152056.0 / 13.0, 105410.0 / 6.0, 83.0, 502731.0 / 6.0, 678110.0 / 10.0,
        830.0 / 3.0, 34445.0 / 7.0, 32619.0 / 7.0, 490198.0 / 4.0, 60839.0 / 10.0
      )
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
        now += (s"${i}" -> (tmp.toDouble / normalization(i)))
      }

      idx -> now
    })
  }
}
