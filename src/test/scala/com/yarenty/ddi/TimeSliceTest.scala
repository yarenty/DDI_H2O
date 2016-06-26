package com.yarenty.ddi

import org.scalatest.FlatSpec


/**
  * Created by yarenty on 26/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class TimeSliceTest extends FlatSpec {

  val t1 = "2016-01-01 00:02:30"
  val slice1 = DataMunging.getTimeSlice(t1)



  s"Slice for ${t1}" should "be 1" in {
    assert(slice1 == 1)
  }


}
