package com.yarenty.ddi.schemas

import water.parser.{DefaultParserProviders, ParseSetup}

/**
  * Created by yarenty on 30/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class OutputLine(
                val timeslice: Int,
                val districtID: Int,
                val gap: Int,
                val predict: Float
              ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Order]

  override def productArity: Int = 4

  override def productElement(n: Int) = n match {
    case 0 => timeslice
    case 1 => districtID
    case 2 => gap
    case 3 => predict
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
object OutputLineParse extends Serializable {
  def apply(row: Array[String]): OutputLine = {

    import water.support.ParseSupport._

    new OutputLine(
      int(row(0)).get, //id
      int(row(1)).get, //timeslice
      int(row(2)).get, //district ID
      float(row(3)).get //destDistrict

    )
  }
}
