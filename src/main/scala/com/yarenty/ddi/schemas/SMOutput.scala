package com.yarenty.ddi.schemas

import water.parser.{DefaultParserProviders, ParseSetup}

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class SMOutput(
                val id: Option[Int],
                val timeslice: Option[Int],
                val districtID: Option[Int],
                val destDistrict: Option[Int],
                val demand: Option[Int],
                val gap: Option[Int],
                val traffic1: Option[Int],
                val traffic2: Option[Int],
                val traffic3: Option[Int],
                val traffic4: Option[Int],
                val weather: Option[Int],
                val temp: Option[Float],
                val pollution: Option[Float],
                val p1: Option[Int],
                val p2: Option[Int],
                val p3: Option[Int],
                val p4: Option[Int],
                val p5: Option[Int],
                val p6: Option[Int],
                val p7: Option[Int],
                val p8: Option[Int],
                val p10: Option[Int],
                val p11: Option[Int],
                val p12: Option[Int],
                val p13: Option[Int],
                val p14: Option[Int],
                val p15: Option[Int],
                val p16: Option[Int],
                val p17: Option[Int],
                val p18: Option[Int],
                val p19: Option[Int],
                val p20: Option[Int],
                val p21: Option[Int],
                val p22: Option[Int],
                val p23: Option[Int],
                val p24: Option[Int],
                val p25: Option[Int]
              ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Order]

  override def productArity: Int = 37

  override def productElement(n: Int) = n match {
    case 0 => id
    case 1 => timeslice
    case 2 => districtID
    case 3 => destDistrict
    case 4 => demand
    case 5 => gap
    case 6 => traffic1
    case 7 => traffic2
    case 8 => traffic3
    case 9 => traffic4
    case 10 => weather
    case 11 => temp
    case 12 => pollution
    case 13 => p1
    case 14 => p2
    case 15 => p3
    case 16 => p4
    case 17 => p5
    case 18 => p6
    case 19 => p7
    case 20 => p8
    case 21 => p10
    case 22 => p11
    case 23 => p12
    case 24 => p13
    case 25 => p14
    case 26 => p15
    case 27 => p16
    case 28 => p17
    case 29 => p18
    case 30 => p19
    case 31 => p20
    case 32 => p21
    case 33 => p22
    case 34 => p23
    case 35 => p24
    case 36 => p25
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

/** A dummy csv parser for SMOutput dataset. */
object SMOutputParse extends Serializable {
  def apply(row: Array[String]): SMOutput = {

    import water.support.ParseSupport._

    new SMOutput(
      int(row(0)), //id
      int(row(1)), //timeslice
      int(row(2)), //district ID
      int(row(3)), //destDistrict
      int(row(4)), //demand
      int(row(5)), //gap
      int(row(6)), //traffic1
      int(row(7)), //traffic2
      int(row(8)), //traffic3
      int(row(9)), //traffic4
      int(row(10)), //weather
      float(row(11)), //temp
      float(row(12)), //pollution
      int(row(13)), //p1
      int(row(14)), //p2
      int(row(15)), //p3
      int(row(16)), //p4
      int(row(17)), //p5
      int(row(18)), //p6
      int(row(19)), //p7
      int(row(20)), //p8
      int(row(21)), //p10
      int(row(22)), //p11
      int(row(23)), //p12
      int(row(24)), //p13
      int(row(25)), //p14
      int(row(26)), //p15
      int(row(27)), //p16
      int(row(28)), //p17
      int(row(29)), //p18
      int(row(30)), //p19
      int(row(31)), //p20
      int(row(32)), //p21
      int(row(33)), //p22
      int(row(34)), //p23
      int(row(35)), //p24
      int(row(36)) //p25

    )
  }
}


//parseFiles
//  paths: ["/opt/data/season_1/outtrain/sm_2016-01-22_test","/opt/data/season_1/outtrain/sm_2016-01-26_test","/opt/data/season_1/outtrain/sm_2016-01-30_test","/opt/data/season_1/outtrain/sm_2016-01-28_test","/opt/data/season_1/outtrain/sm_2016-01-24_test"]
//  destination_frame: "sm_2016_01_22_test.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 37
//  single_quotes: false
//  column_names: ["id","timeslice","districtID","destDistrict","demand","gap","traffic1","traffic2","traffic3","traffic4","weather","temp","pollution","1","2","3","4","5","6","7","8","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25"]
//  column_types: ["Numeric","Enum","Enum","Enum","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric","Numeric"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 14781440

//"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p10",
//"p11", "p12", "p13", "p14", "p15", "p16", "p17", "p18", "p19", "p20",
//"p21", "p22", "p23", "p24", "p25")

object SMOutputCSVParser {

  def get: ParseSetup = {
    val parse: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "id", "timeslice", "districtID", "destDistrict", "demand", "gap",
      "traffic1", "traffic2", "traffic3", "traffic4", "weather", "temp", "pollution",
      "1", "2", "3", "4", "5", "6", "7", "8", "10",
      "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
      "21", "22", "23", "24", "25")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "int", "enum", "enum", "enum", "int", "int",
      "float", "float", "float", "float", "float", "float", "float",
      "float", "float", "float", "float", "float", "float", "float", "float", "float", "float",
      "float", "float", "float", "float", "float", "float", "float", "float", "float", "float",
      "float", "float", "float", "float", "float"
    ))
    parse.setColumnNames(orderNames)
    parse.setColumnTypes(orderTypes)
    parse.setParseType(DefaultParserProviders.CSV_INFO)
    parse.setSeparator(44)
    parse.setNumberColumns(7)
    parse.setSingleQuotes(false)
    parse.setCheckHeader(1)
    return parse
  }

}