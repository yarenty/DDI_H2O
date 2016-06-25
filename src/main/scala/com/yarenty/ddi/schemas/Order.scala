package com.yarenty.ddi.schemas

import water.parser._

/**
  * Order definition
  *
  * @param OrderId
  * @param DriverId
  * @param PassengerId
  * @param StartDH
  * @param DestDH
  * @param Price
  * @param Time
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.*
  */
class Order(val OrderId: Option[String],
            val DriverId: Option[String],
            val PassengerId: Option[String],
            val StartDH: Option[String],
            val DestDH: Option[String],
            val Price: Option[Float],
            val Time: Option[String]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Order]

  override def productArity: Int = 7

  override def productElement(n: Int) = n match {
    case 0 => OrderId
    case 1 => DriverId
    case 2 => PassengerId
    case 3 => StartDH
    case 4 => DestDH
    case 5 => Price
    case 6 => Time
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
object OrderParse extends Serializable {
  def apply(row: Array[String]): Order = {

    import water.support.ParseSupport._

    new Order(str(row(0)), // order
      str(row(1)), // driver
      str(row(2)), // passenger
      str(row(3)), // start
      str(row(4)), // dest
      float(row(5)), // price
      str(row(6)) // time

    )
  }
}



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

object OrderCSVParser {

  def get:ParseSetup = {
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
    return parseOrders
  }

}
