package com.yarenty.ddi.schemas

/**
  * Simple district mapping
  *
  * @param DistrictHash
  * @param DistrictID
  *
  * Created by yarenty on 24/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
case class District(val DistrictHash: Option[String],
                    val DistrictID: Option[Int]) {
  def isWrongRow(): Boolean = (0 until productArity).map(idx => productElement(idx)).forall(e => e == None)
}

object DistrictParse extends Serializable {

  import water.support.ParseSupport._

  def apply(row: Array[String]): District = {
    District(
      str(row(0)),
      int(row(1))
    )
  }
}
