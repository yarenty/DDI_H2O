package com.yarenty.ddi

import org.scalatest.FlatSpec


/**
  * Created by yarenty on 26/06/2016.
  * (C)2015 SkyCorp Ltd.
  */
class IndexingTest extends FlatSpec {


  val idx1 = DataMunging.getIndex(1,1,1)

  s"Index for 1,1,1," should "be 10101" in {
    assert(idx1 == 10101)
  }

  val idx2 = DataMunging.getIndex(144,66,66)
  s"Index for 144,66,66," should "be 1446666" in {
    assert(idx2 == 1446666)
  }

  val bactdIdx1 = DataMunging.getBack(10101)
  s"Index for bactdIdx 10101," should "be (1,1,1)" in {
    assert(bactdIdx1 == (1,1,1))
  }

  val bactdIdx2 = DataMunging.getBack(1446666)
  s"Index for bactdIdx 1446666," should "be (144,66,66)" in {
    assert(bactdIdx2 == (144,66,66))
  }

}
