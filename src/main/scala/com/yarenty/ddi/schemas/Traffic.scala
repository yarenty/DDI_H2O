package com.yarenty.ddi.schemas

import water.parser._

/**                                          ß
  * Traffic schema definition
 *
  * @param DistrictHash
  * @param Traffic1
  * @param Traffic2
  * @param Traffic3
  * @param Traffic4
  * @param Time
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.  *
  */
class TrafficIN(val DistrictHash: Option[String],
            val Traffic1: Option[String],
            val Traffic2: Option[String],
            val Traffic3: Option[String],
            val Traffic4: Option[String],
            val Time: Option[String]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[TrafficIN]
  override def productArity: Int = 6
  override def productElement(n: Int) = n match {
    case 0 => DistrictHash
    case 1 => Traffic1
    case 2 => Traffic2
    case 3 => Traffic3
    case 4 => Traffic4
    case 5 => Time
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
object TrafficINParse extends Serializable {

  def apply(row: Array[String]): TrafficIN = {
    import water.support.ParseSupport._

    new TrafficIN(str(row(0)), // district
      str(row(1)), // t1
      str(row(2)), // t2
      str(row(3)), // t3
      str(row(4)), // t4
      str(row(5)) // time

    )
  }
}



/**
  * Traffic schema definition
 *
  * @param DistrictHash
  * @param Traffic1
  * @param Traffic2
  * @param Traffic3
  * @param Traffic4
  * @param Time
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.  *
  */
class Traffic(val DistrictHash: Option[String],
            val Traffic1: Option[Int],
            val Traffic2: Option[Int],
            val Traffic3: Option[Int],
            val Traffic4: Option[Int],
            val Time: Option[String]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Traffic]
  override def productArity: Int = 6
  override def productElement(n: Int) = n match {
    case 0 => DistrictHash
    case 1 => Traffic1
    case 2 => Traffic2
    case 3 => Traffic3
    case 4 => Traffic4
    case 5 => Time
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


///** A dummy csv parser for orders dataset. */
object TrafficParse extends Serializable {

  def apply(row: TrafficIN): Traffic = {
    import water.support.ParseSupport._

    val traffic1 = Option(row.Traffic1.get.split(':')(1).toInt)
    val traffic2 = Option(row.Traffic2.get.split(':')(1).toInt)
    val traffic3 = Option(row.Traffic3.get.split(':')(1).toInt)
    val traffic4 = Option(row.Traffic4.get.split(':')(1).toInt)

    new Traffic(row.DistrictHash, // district
      traffic1, // t1
      traffic2, // t2
      traffic3, // t3
      traffic4, // t4
      row.Time // time
    )
  }
}



object TrafficCSVParser{
  def get:ParseSetup= {
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
    val trafficNames: Array[String] = Array(
      "DistrictHash", "Traffic1", "Traffic2", "Traffic3", "Traffic4", "Time")
    val trafficTypes = ParseSetup.strToColumnTypes(Array(
      "string", "string", "string", "string", "string", "string"))
    parseTraffic.setColumnNames(trafficNames)
    parseTraffic.setColumnTypes(trafficTypes)
    parseTraffic.setParseType(DefaultParserProviders.CSV_INFO)
    parseTraffic.setSeparator('\t')
    parseTraffic.setNumberColumns(6)
    parseTraffic.setSingleQuotes(false)
    parseTraffic.setCheckHeader(-1)
  }
}
