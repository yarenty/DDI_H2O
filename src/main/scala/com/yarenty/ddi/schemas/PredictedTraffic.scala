package com.yarenty.ddi.schemas

import water.parser._

/**
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.  *
  */
class PTraffic(val district: Option[Int],
               val timeslice: Option[Int],
                val t1: Option[Double],
                val t2: Option[Double],
                val t3: Option[Double],
                val t4: Option[Double],
                val t1p: Option[Double],
                val t2p: Option[Double],
                val t3p: Option[Double],
                val t4p: Option[Double]

              ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[TrafficIN]

  override def productArity: Int = 10

  override def productElement(n: Int) = n match {
    case 0 => district
    case 1 => timeslice
    case 2 => t1
    case 3 => t2
    case 4 => t3
    case 5 => t4
    case 6 => t1p
    case 7 => t2p
    case 8 => t3p
    case 9 => t4p
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def toString: String = {
    val sb = new StringBuffer
    for (i <- 0 until productArity)
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow(): Boolean = (0 until productArity)
    .map(idx => productElement(idx))
    .forall(e => e == None)
}

/** A dummy csv parser for orders dataset. */
object PTrafficParse extends Serializable {

  def apply(row: Array[String]): PTraffic = {
    import water.support.ParseSupport._

    new PTraffic(
      int(row(0)), // district
      int(row(1)), // t1
      Option(row(2).trim().toDouble),
      Option(row(3).trim().toDouble),
      Option(row(4).trim().toDouble),
      Option(row(5).trim().toDouble),
      Option(row(6).trim().toDouble),
      Option(row(7).trim().toDouble),
      Option(row(8).trim().toDouble),
      Option(row(9).trim().toDouble)

    )
  }
}



/**
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.  *
  *  "day","timeslice","weather","temp","pollution")
  */
class PWeather(val day: Option[Int],
               val timeslice: Option[Int],
                val weather: Option[Int],
                val temp: Option[Double],
                val pollution: Option[Double]

              ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[TrafficIN]

  override def productArity: Int = 5

  override def productElement(n: Int) = n match {
    case 0 => day
    case 1 => timeslice
    case 2 => weather
    case 3 => temp
    case 4 => pollution
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def toString: String = {
    val sb = new StringBuffer
    for (i <- 0 until productArity)
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow(): Boolean = (0 until productArity)
    .map(idx => productElement(idx))
    .forall(e => e == None)
}



/** A dummy csv parser for orders dataset. */
object PWeatherParse extends Serializable {

  def apply(row: Array[String]): PWeather = {
    import water.support.ParseSupport._

    new PWeather(
      int(row(0)), // district
      int(row(1)), // t1
      int(row(2)),
      Option(row(3).trim().toDouble),
      Option(row(4).trim().toDouble)
    )
  }
}


object WCVSParser {
  def get: ParseSetup = {

    val parseTraffic: ParseSetup = new ParseSetup()
    val trafficNames: Array[String] = Array(
      "day","timeslice","weather","temp","pollution")
    val trafficTypes = ParseSetup.strToColumnTypes(Array(
      "int","int",  "int","double","double"))
    parseTraffic.setColumnNames(trafficNames)
    parseTraffic.setColumnTypes(trafficTypes)
    parseTraffic.setParseType(DefaultParserProviders.CSV_INFO)
    parseTraffic.setSeparator(',')
    parseTraffic.setNumberColumns(5)
    parseTraffic.setSingleQuotes(false)
    parseTraffic.setCheckHeader(1)
  }

}

object TCSVParser {
  def get: ParseSetup = {

    val parseTraffic: ParseSetup = new ParseSetup()
    val trafficNames: Array[String] = Array(
      "district","timeslice","t1","t2","t3","t4","t1p","t2p","t3p","t4p")
    val trafficTypes = ParseSetup.strToColumnTypes(Array(
      "int","int",  "int","int","int","int", "double","double","double","double"))
    parseTraffic.setColumnNames(trafficNames)
    parseTraffic.setColumnTypes(trafficTypes)
    parseTraffic.setParseType(DefaultParserProviders.CSV_INFO)
    parseTraffic.setSeparator(',')
    parseTraffic.setNumberColumns(10)
    parseTraffic.setSingleQuotes(false)
    parseTraffic.setCheckHeader(1)
  }
}
