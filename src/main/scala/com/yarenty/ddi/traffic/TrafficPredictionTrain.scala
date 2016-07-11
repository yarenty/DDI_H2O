package com.yarenty.ddi.traffic

import java.io.{File, PrintWriter}

import com.yarenty.ddi.raw.DataMunging
import DataMunging._
import com.yarenty.ddi.schemas._
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, h2o}
import water.fvec.{AppendableVec, Frame, NewChunk, Vec}
import water.support.SparkContextSupport
import water.{Futures, Key}

/**
  * Created by yarenty on 06/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object TrafficPredictionTrain extends SparkContextSupport {

  val data_dir = "/opt/data/season_1/"
  val output_dir = "/opt/data/season_1/outtraffic/t_2016-01-"

  var traffic_csv = ""
  var weather_csv = ""


  val districts: Map[String, Int] = {
    Map(
      "7f84bdfc2b6d4541e1f6c0a3349e0251" -> 52,
      "fff4e8465d1e12621bc361276b6217cf" -> 32,
      "58c7a4888306d8ff3a641d1c0feccbe3" -> 3,
      "74ec84f1cf75cf89ae176c8c6ceec5ba" -> 49,
      "c4ec24e0a58ebedaa1661e5c09e47bb5" -> 54,
      "91690261186ae5bee8f83808ea1e4a01" -> 20,
      "f9280c5dab6910ed44e518248048b9fe" -> 41,
      "73ff8ef735e1d68f0cdcbb84d788f2b6" -> 40,
      "4f4041f7db0c7f69892d9b74c1a7efa1" -> 10,
      "4725c39a5e5f4c188d382da3910b3f3f" -> 23,
      "62afaf3288e236b389af9cfdc5206415" -> 48,
      "1afd7afbc81ecc1b13886a569d869e8a" -> 46,
      "0a5fef95db34383403d11cb6af937309" -> 63,
      "2350be163432e42270d2670cb3c02f80" -> 18,
      "fc34648599753c9e74ab238e9a4a07ad" -> 27,
      "38d5ad2d22b61109fd8e7b43cd0e8901" -> 24,
      "90c5a34f06ac86aee0fd70e2adce7d8a" -> 1,
      "d4ec2125aff74eded207d2d915ef682f" -> 51,
      "82cc4851f9e4faa4e54309f8bb73fd7c" -> 8,
      "ca064c2682ca48c6a21de012e87c0df5" -> 42,
      "a814069db8d32f0fa6e188f41059c6e1" -> 17,
      "1c60154546102e6525f68cb4f31e0657" -> 56,
      "4b9e4cf2fbdc8281b8a1f9f12b80ce4d" -> 5,
      "1cbfbdd079ef93e74405c53fcfff8567" -> 6,
      "364bf755f9b270f0f9141d1a61de43ee" -> 21,
      "52d7b69796362a8ed1691a6cc02ddde4" -> 33,
      "307afa4120c590b3a46cf4ff5415608a" -> 16,
      "d05052b4bda7662a084f235e880f50fa" -> 36,
      "bf44d327f0232325c6d5280926d7b37d" -> 64,
      "08f5b445ec6b29deba62e6fd8b0325a6" -> 43,
      "3a43dcdff3c0b66b1acb1644ff055f9d" -> 25,
      "a735449c5c09df639c35a7d61fad3ee5" -> 62,
      "44c097b7bd219d104050abbafe51bd49" -> 35,
      "ba32abfc048219e933bee869741da911" -> 57,
      "d524868ce69cb9db10fc5af177fb9423" -> 59,
      "445ff793ebd3477d4a2e0b36b2db9271" -> 55,
      "2920ece99323b4c111d6f9affc7ea034" -> 14,
      "8bb37d24db1ad665e706c2655d9c4c72" -> 34,
      "f2c8c4bb99e6377d21de71275afd6cd2" -> 2,
      "a5609739c6b5c2719a3752327c5e33a7" -> 19,
      "de092beab9305613aca8f79d7d7224e7" -> 61,
      "08232402614a9b48895cc3d0aeb0e9f2" -> 50,
      "2407d482f0ffa22a947068f2551fe62c" -> 28,
      "1ecbb52d73c522f184a6fc53128b1ea1" -> 66,
      "929ec6c160e6f52c20a4217c7978f681" -> 7,
      "b26a240205c852804ff8758628c0a86a" -> 4,
      "52a4e8aaa12f70020e889aed8fd5ddbc" -> 29,
      "52e56004d92b8c74d53e1e42699cba6f" -> 26,
      "4f8d81b5c31af5d1ba579a65ddc8a5cb" -> 38,
      "87285a66236346350541b8815c5fae94" -> 22,
      "825c426141df01d38c1b9e9c5330bdac" -> 30,
      "49ac89aa860c27e26c0836cb8dab2df2" -> 60,
      "8316146a6f78cc6d9f113f0390859417" -> 44,
      "b702e920dcd2765e624dc1ce3a770512" -> 9,
      "74c1c25f4b283fa74a5514307b0d0278" -> 12,
      "2301bc920194c95cf0c7486e5675243c" -> 31,
      "4b7f6f4e2bf237b6cc58f57142bea5c0" -> 13,
      "d5cb17978de290c56e84c9cf97e63186" -> 15,
      "b05379ac3f9b7d99370d443cfd5dcc28" -> 37,
      "825a21aa308dea206adb49c4b77c7805" -> 65,
      "cb6041cc08444746caf6039d8b9e43cb" -> 58,
      "c9f855e3e13480aad0af64b418e810c3" -> 45,
      "693a21b16653871bbd455403da5412b4" -> 39,
      "f47f35242ed40655814bc086d7514046" -> 53,
      "dd8d3b9665536d6e05b29c2648c0e69a" -> 11,
      "3e12208dd0be281c92a6ab57d9a6fb32" -> 47
    )

  }


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)

    addFiles(h2oContext.sparkContext,
      absPath(poi_csv)
    )

    // Use super-fast advanced H2O CSV parser !!!
    val poiData = new h2o.H2OFrame(new File(SparkFiles.get("poi_data")))
    println(s"\n===> POI via H2O#Frame#count: ${poiData.numRows()}\n")
    val poi: Map[Int, Map[String, Int]] = asRDD[POI](poiData).map(row => {
      TrafficPrediction.getPOIMap(districts, row)
    }).collect().toMap
    val mergedPOI: Map[Int, Map[String, Double]] = TrafficPrediction.mergePOI(poi)
    for (m <- mergedPOI) {
      println(m)
    }

    val files = {

      traffic_csv = data_dir + "training_data/traffic_data/traffic_data_"
      weather_csv = data_dir + "outweather/w_"

      (1 to 21).map(i => {
        val pd = "2016-01-" + "%02d".format(i)
        (i, traffic_csv + pd, weather_csv + pd, pd)
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


      var traffic: Map[Int, Tuple7[Int, Int, Int, Int, Int, Int, Int]] = trafficTable.map(row => {
        val ts = getTimeSlice(row.Time.get)
        if (ts < 1) println(s" WRONG TIME: ${row.Time} ")
        val din = districts.get(row.DistrictHash.get).get
        val t1 = row.Traffic1.get
        val t2 = row.Traffic2.get
        val t3 = row.Traffic3.get
        val t4 = row.Traffic4.get
        ts * 100 + din ->(PROCESSED_DAY % 7, din, ts, t1, t2, t3, t4)
      }).collect().toMap
      println(s" TRAFFIC MAP SIZE: ${traffic.size}")


      var filledTraffic: Tuple7[Int, Int, Int, Int, Int, Int, Int] = (0, 0, 0, 0, 0, 0, 0)
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

      val csv_writer = new PrintWriter(new File(output_dir + "%02d".format(PROCESSED_DAY)))
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
}
