package com.yarenty.ddi.weather

import java.io.{PrintWriter, File}

import com.yarenty.ddi.DataMunging._
import com.yarenty.ddi.schemas.{WeatherParse, WeatherIN, Weather, WeatherCSVParser}
import org.apache.spark.{SparkFiles, h2o}
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import water.{Key, Futures}
import water.fvec.{Frame, Vec, NewChunk, AppendableVec}
import water.support.SparkContextSupport

/**
  * Created by yarenty on 06/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object WeatherPrediction extends SparkContextSupport {

  val data_dir = "/opt/data/season_1/"
  val output_dir = "/opt/data/season_1/outweather/w_2016-01-"

  var weather_csv = ""


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val files = {

//      weather_csv = data_dir + "training_data/weather_data/weather_data_"
//
//      val out: Seq[Tuple2[Int, String]] =
//        (1 to 21).map(i => {
//          val pd = "2016-01-" + "%02d".format(i)
//          (i, weather_csv + pd)
//        }).toSeq

            weather_csv = data_dir + "test_set_1/weather_data/weather_data_"
            val a = Array("2016-01-22_test", "2016-01-24_test", "2016-01-26_test", "2016-01-28_test", "2016-01-30_test")
            var x = 20

           // out ++
              a.map(pd => {
              x += 2
              (x, weather_csv + pd)
            })


    }


    for (f <- files) {
      val PROCESSED_DAY = f._1

      println(s"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\nSTART PROCESSING : ${PROCESSED_DAY}")

      addFiles(sc,
        absPath(f._2)
      )


      val weatherData = new h2o.H2OFrame(WeatherCSVParser.get,
        new File(SparkFiles.get("weather_data_" + "2016-01-" + "%02d".format(PROCESSED_DAY) + "_test"))) // Use super-fast advanced H2O CSV parser !!!
      println(s"\n===> WEATHER via H2O#Frame#count: ${weatherData.numRows()}\n")
      val weatherTable: h2o.RDD[Weather] = asRDD[WeatherIN](weatherData)
        .map(row => WeatherParse(row)).filter(!_.isWrongRow)
      println(s"\n===> WEATHER in ${f._2} via RDD#count call: ${weatherTable.count()}\n")

      //: Map[Int, Tuple4[Int,Int, Double, Double]]
      var weather = weatherTable.map(row => {
        row.ts ->(PROCESSED_DAY, row.ts, row.Weather.get, row.Temperature.get, row.Pollution.get)
      }).collect().toMap
      println(s" WEATHER MAP SIZE: ${weather.size}")


      val headers = Array("day", "timeslice", "weather", "temp", "pollution")

      val out = new h2o.H2OFrame(lineBuilder(headers, weather, PROCESSED_DAY))

      val csv = out.toCSV(true, false)

      val csv_writer = new PrintWriter(new File(output_dir + "%02d".format(PROCESSED_DAY) + "_predict"))
      while (csv.available() > 0) {
        csv_writer.write(csv.read.toChar)
      }
      csv_writer.close

      out.delete()
      weatherTable.delete()
      weatherData.delete()
    }


  }

  def lineBuilder(headers: Array[String], weather: Map[Int,(Int, Int, Int, Double, Double)], pd:Int): Frame = {

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
      if (weather.contains(ts)) {
        val w = weather.get(ts).get
        chunks(0).addNum(w._1)
        chunks(1).addNum(w._2)
        chunks(2).addNum(w._3)
        chunks(3).addNum(w._4)
        chunks(4).addNum(w._5)
      }
      else {
        chunks(0).addNum(pd)
        chunks(1).addNum(ts)
        chunks(2).addNA()
        chunks(3).addNA()
        chunks(4).addNA()
      }
    }


    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }
    val key = Key.make("Weather")
    return new Frame(key, headers, vecs)
  }


}