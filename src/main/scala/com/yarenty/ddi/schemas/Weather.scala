package com.yarenty.ddi.schemas

/**
  * Weather schema definition.
  * @param Time
  * @param Weather  1-night, 8-sunny, 4-rain, etc...
  * @param Temperature
  * @param Pollution
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class Weather(val Time: Option[String],
            val Weather: Option[Int],
            val Temperature: Option[Float],
            val Pollution: Option[Float]) extends Product with Serializable {

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
object WeatherParse extends Serializable {

  def apply(row: Array[String]): Weather = {
    import water.support.ParseSupport._

    new Weather(str(row(0)), // time
      int(row(1)), // wether: 1 night, 8 sunny, 4 rain etc.
      float(row(2)), // temp
      float(row(3)) // pollution
    )
  }
}
