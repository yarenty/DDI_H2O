package com.yarenty.ddi.schemas

import com.yarenty.ddi.DataMunging
import water.parser.{DefaultParserProviders, ParseSetup}

/**
  * Weather schema definition.
  *
  * @param ts
  * @param Weather 1-night, 8-sunny, 4-rain, etc...
  * @param Temperature
  * @param Pollution
  *
  *                Created by yarenty on 24/06/2016.
  *                (C)2015 SkyCorp Ltd.
  */
class Weather(val ts: Int,
              val Weather: Option[Int],
              val Temperature: Option[Double],
              val Pollution: Option[Double]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Weather]

  override def productArity: Int = 4

  override def productElement(n: Int) = n match {
    case 0 => ts
    case 1 => Weather
    case 2 => Temperature
    case 3 => Pollution
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def toString: String = {
    val sb = new StringBuffer
    for (i <- 0 until productArity)
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow(): Boolean = (0 until productArity).map(idx => productElement(idx)).forall(e => e == None)
}

/** A dummy csv parser for orders dataset. */
object WeatherParse extends Serializable {

  def apply(row: WeatherIN): Weather = {
    import water.support.ParseSupport._

    new Weather(
      DataMunging.getTimeSlice(row.Time.get), // time
      row.Weather, // wether: 1 night, 8 sunny, 4 rain etc.
      row.Temperature, // temp
      row.Pollution // pollution
    )
  }


}


class WeatherIN(val Time: Option[String],
                val Weather: Option[Int],
                val Temperature: Option[Double],
                val Pollution: Option[Double]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Weather]

  override def productArity: Int = 4

  override def productElement(n: Int) = n match {
    case 0 => Time
    case 1 => Weather
    case 2 => Temperature
    case 3 => Pollution
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def toString: String = {
    val sb = new StringBuffer
    for (i <- 0 until productArity)
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow(): Boolean = (0 until productArity).map(idx => productElement(idx)).forall(e => e == None)
}

/** A dummy csv parser for orders dataset. */
object WeatherINParse extends Serializable {

  def apply(row: Array[String]): WeatherIN = {
    import water.support.ParseSupport._

    new WeatherIN(
      str(row(0)), // time
      int(row(1)), // wether: 1 night, 8 sunny, 4 rain etc.
      Option(row(2).trim().toDouble), // temp
      Option(row(3).trim().toDouble) // pollution
    )
  }
}

object WeatherCSVParser {

  def get: ParseSetup = {
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
    val weatherNames: Array[String] = Array("Time", "Weather", "Temperature", "Pollution")
    val weatherTypes = ParseSetup.strToColumnTypes(Array("string", "int", "double", "double"))
    parseWeather.setColumnNames(weatherNames)
    parseWeather.setColumnTypes(weatherTypes)
    parseWeather.setParseType(DefaultParserProviders.CSV_INFO)
    parseWeather.setSeparator('\t')
    parseWeather.setNumberColumns(6)
    parseWeather.setSingleQuotes(false)
    parseWeather.setCheckHeader(-1)
    return parseWeather
  }

}
